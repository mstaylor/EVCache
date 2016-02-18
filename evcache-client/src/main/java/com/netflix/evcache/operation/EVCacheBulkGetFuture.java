package com.netflix.evcache.operation;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.sun.management.GcInfo;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.servo.monitor.Stopwatch;

/**
 * Future for handling results from bulk gets.
 *
 * Not intended for general use.
 *
 * types of objects returned from the GETBULK
 */
public class EVCacheBulkGetFuture<T> extends BulkGetFuture<T> {

    private Logger log = LoggerFactory.getLogger(EVCacheBulkGetFuture.class);
    private final Map<String, Future<T>> rvMap;
    private final Collection<Operation> ops;
    private final CountDownLatch latch;
    private final String appName;
    private final ServerGroup serverGroup;
    private final Stopwatch operationDuration;

    public EVCacheBulkGetFuture(String appName, Map<String, Future<T>> m, Collection<Operation> getOps, CountDownLatch l, ExecutorService service, ServerGroup serverGroup) {
        super(m, getOps, l, service);
        this.appName = appName;
        rvMap = m;
        ops = getOps;
        latch = l;
        this.serverGroup = serverGroup;
        this.operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "LatencyBulk").start();
    }

    public Map<String, T> getSome(long to, TimeUnit unit, boolean throwException, boolean hasZF)
            throws InterruptedException, ExecutionException {
        final Collection<Operation> timedoutOps = new HashSet<Operation>();

        final long startTime = System.currentTimeMillis();
        boolean status = latch.await(to, unit);

        if (!status) {
            boolean gcPause = false;
            final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            final long vmStartTime = runtimeBean.getStartTime();
            final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
            for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                if (gcMXBean instanceof com.sun.management.GarbageCollectorMXBean) {
                    final GcInfo lastGcInfo = ((com.sun.management.GarbageCollectorMXBean) gcMXBean).getLastGcInfo();

                    // If no GCs, there was no pause.
                    if (lastGcInfo == null) {
                        continue;
                    }

                    final long gcStartTime = lastGcInfo.getStartTime() + vmStartTime;
                    if (gcStartTime > startTime) {
                        gcPause = true;
                        final long gcDuration = lastGcInfo.getDuration();
                        EVCacheMetricsFactory.getCounter(appName + "-DelayDueToGCPause").increment(gcDuration);
                        if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + gcDuration
                                + " msec.");
                        break;
                    }
                }
            }
            if (!gcPause) {
                long gcDuration = System.currentTimeMillis() - startTime;
                EVCacheMetricsFactory.getLongGauge(appName + "-DelayProbablyDueToGCPause").set(Long.valueOf(
                        gcDuration));
            }
            // redo the same op once more since there was a chance of gc pause
            if (gcPause) {
                status = latch.await(to, unit);
                if (log.isDebugEnabled()) log.debug("Retry status : " + status);
                if (status) {
                    EVCacheMetricsFactory.increment(appName + "-DelayDueToGCPause-Success");
                } else {
                    EVCacheMetricsFactory.increment(appName + "-DelayDueToGCPause-Fail");
                }
            }
            if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + (System.currentTimeMillis()
                    - startTime) + " msec.");
        }

        for (Operation op : ops) {
            if (op.getState() != OperationState.COMPLETE) {
                if (!status) {
                    MemcachedConnection.opTimedOut(op);
                    timedoutOps.add(op);
                } else {
                    MemcachedConnection.opSucceeded(op);
                }
            } else {
                MemcachedConnection.opSucceeded(op);
            }
        }

        if (!status && !hasZF && timedoutOps.size() > 0) EVCacheMetricsFactory.increment(appName
                + "-getSome-CheckedOperationTimeout");

        for (Operation op : ops) {
            if (op.isCancelled() && throwException) throw new ExecutionException(new CancellationException(
                    "Cancelled"));
            if (op.hasErrored() && throwException) throw new ExecutionException(op.getException());
        }
        Map<String, T> m = new HashMap<String, T>();
        for (Map.Entry<String, Future<T>> me : rvMap.entrySet()) {
            m.put(me.getKey(), me.getValue().get());
        }
        return m;
    }
    
    public String getZone() {
        return (serverGroup == null ? "NA" : serverGroup.getZone());
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public String getApp() {
        return appName;
    }

    public Set<String> getKeys() {
        return Collections.unmodifiableSet(rvMap.keySet());
    }

    public void signalComplete() {
        if (operationDuration != null) operationDuration.stop();
        super.signalComplete();
    }

}