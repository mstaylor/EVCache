package com.netflix.evcache.pool;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.DistributionSummary;

import net.spy.memcached.CachedData;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.StatusCode;

public class EVCacheClientUtil {
    private static Logger log = LoggerFactory.getLogger(EVCacheClientUtil.class);
    private final ChunkTranscoder ct = new ChunkTranscoder();
    private final String _appName;
    private final DistributionSummary addDataSizeSummary;
    private final DistributionSummary addTTLSummary;
    private final DynamicBooleanProperty fixup;
    private final EVCacheClientPool _pool;
    private ThreadPoolExecutor threadPool = null;

    public EVCacheClientUtil(EVCacheClientPool pool) {
        this._pool = pool;
        this._appName = pool.getAppName();
        this.addDataSizeSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-AddData-Size", _appName, null);
        this.addTTLSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-AddData-TTL", _appName, null);
        this.fixup = EVCacheConfig.getInstance().getDynamicBooleanProperty(_appName + ".addOperation.fixup", Boolean.FALSE);
        int maxThreads = 10;

        RejectedExecutionHandler block = new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    executor.getQueue().put( r );
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(1000);
        threadPool = new ThreadPoolExecutor(maxThreads, maxThreads * 2, 30, TimeUnit.SECONDS, queue);
        threadPool.setRejectedExecutionHandler(block);
        threadPool.prestartAllCoreThreads();
        
    }

    public EVCacheLatch add(String canonicalKey, CachedData cd, int timeToLive, Policy policy) throws Exception {
        if (cd == null) return null; 
        addDataSizeSummary.record(cd.getData().length);
        addTTLSummary.record(timeToLive);
        
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName){

            @Override
            public void onComplete(OperationFuture<?> operationFuture) throws Exception {
                super.onComplete(operationFuture);
                if (getPendingFutureCount() == 0 && fixup.get()) {
                    final RemoteRequest req = new RemoteRequest(this, canonicalKey, timeToLive);
                    threadPool.submit(req);
                }
            }
        };

        for (EVCacheClient client : clients) {
            final Future<Boolean> future = client.add(canonicalKey, timeToLive, cd, ct, latch);
            if(log.isDebugEnabled()) log.debug("ADD : Op Submitted : APP " + _appName + ", key " + canonicalKey + "; future : " + future);
        }
        return latch;
    }

    class RemoteRequest implements Runnable {
        private EVCacheLatchImpl latch;
        private String canonicalKey;
        private int timeToLive;
        public RemoteRequest(EVCacheLatchImpl latch, String canonicalKey, int timeToLive) {
            this.latch = latch;
            this.canonicalKey = canonicalKey;
            this.timeToLive = timeToLive;
        }
        public void run() {
            final List<Future<Boolean>> futures = latch.getAllFutures();
            int successCount = 0, failCount = 0;
            for(int i = 0; i < futures.size() ; i++) {
                final Future<Boolean> future = futures.get(i);
                if(future instanceof EVCacheOperationFuture) {
                    final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)future;
                    if(f.getStatus().getStatusCode() == StatusCode.SUCCESS) {
                        successCount++;
                        if(log.isDebugEnabled()) log.debug("ADD : Success : APP " + _appName + ", key " + canonicalKey+ ", ServerGroup : " + f.getServerGroup().getName());
                    } else {
                        failCount++;
                        if(log.isDebugEnabled()) log.debug("ADD : Fail : APP " + _appName + ", key : " + canonicalKey + ", ServerGroup : " + f.getServerGroup().getName());
                    }
                }
            }
            if(log.isDebugEnabled()) log.debug("ADD : Status: APP " + _appName + ", key : " + canonicalKey + ", failCount : " + failCount + "; successCount : " + successCount);

            if(successCount > 0 && failCount > 0) {
                CachedData readData = null;
                for(int i = 0; i < futures.size(); i++) {
                    final Future<Boolean> evFuture = futures.get(i);
                    if(evFuture instanceof EVCacheOperationFuture) {
                        final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)evFuture;
                        if(f.getStatus().getStatusCode() == StatusCode.ERR_EXISTS) {
                            final EVCacheClient client = _pool.getEVCacheClient(f.getServerGroup());
                            if(client != null) {
                                try {
                                    readData = client.get(canonicalKey, ct, false, false);
                                } catch (Exception e) {
                                    log.error("Error readig the data", e);
                                }
                                if(log.isDebugEnabled()) log.debug("Add : Read existing data for: APP " + _appName + ", key " + canonicalKey + "; ServerGroup : " + client.getServerGroupName());
                                if(readData != null) {
                                    break;
                                } else {
                                    
                                }
                            }
                        }
                    }
                }
                if(readData != null) {
                    for(int i = 0; i < futures.size(); i++) {
                        final Future<Boolean> evFuture = futures.get(i);
                        if(evFuture instanceof OperationFuture) {
                            final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)evFuture;
                            if(f.getStatus().getStatusCode() == StatusCode.SUCCESS) {
                                final EVCacheClient client = _pool.getEVCacheClient(f.getServerGroup());
                                if(client != null) {
                                    try {
                                        client.set(canonicalKey, readData, timeToLive, null);
                                        if(log.isDebugEnabled()) log.debug("Add: Fixup for : APP " + _appName + ", key " + canonicalKey + "; ServerGroup : " + client.getServerGroupName());
                                        EVCacheMetricsFactory.increment(_appName , null, client.getServerGroupName(), _appName + "-AddCall-FixUp");
                                    } catch (Exception e) {
                                        if(log.isDebugEnabled()) log.debug("Add: Fixup Error : APP " + _appName + ", key " + canonicalKey + "; ServerGroup : " + client.getServerGroupName(), e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
