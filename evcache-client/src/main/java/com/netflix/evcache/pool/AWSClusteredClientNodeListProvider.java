package com.netflix.evcache.pool;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.config.NodeEndPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A Node List Provider the Leverages the AWS Clustered Client for Node Discovery.
 *
 * @author Mills W. Staylor, III
 */
public class AWSClusteredClientNodeListProvider implements EVCacheNodeList{

    private static Logger log = LoggerFactory.getLogger(EVCacheClientPool.class);

    private String currentNodeList = "";

    private final String propertyName;

    public AWSClusteredClientNodeListProvider(String propertyName) {
        this.propertyName = propertyName;
    }


    @Override
    public Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances() throws IOException {

        final String nodeListString = System.getProperty(propertyName);
        if (log.isInfoEnabled()) {
            log.info("List of Nodes: {}", nodeListString);
        }
        if (nodeListString != null && nodeListString.length() > 0) {
            final Map<ServerGroup, EVCacheServerGroupConfig> instancesSpecific = new HashMap<>();
            final StringTokenizer setTokenizer = new StringTokenizer(nodeListString, ";");
            while (setTokenizer.hasMoreTokens()) {
                final String token = setTokenizer.nextToken();
                final StringTokenizer replicaSetTokenizer = new StringTokenizer(token, "=");
                while (replicaSetTokenizer.hasMoreTokens()) {
                    final String replicaSetToken = replicaSetTokenizer.nextToken();
                    final String cacheClusterToken = replicaSetTokenizer.nextToken();
                    final Set<InetSocketAddress> instanceList = new HashSet<>();
                    final ServerGroup rSet = new ServerGroup(replicaSetToken, replicaSetToken);
                    final EVCacheServerGroupConfig config = new EVCacheServerGroupConfig(rSet, instanceList, 0, 0, 0, true);
                    instancesSpecific.put(rSet, config);
                    final int index = cacheClusterToken.indexOf(':');
                    final String host = cacheClusterToken.substring(0, index);
                    final String port = cacheClusterToken.substring(index + 1);
                    Map<String, InetSocketAddress> mappedNodes;
                    if (log.isInfoEnabled()) {
                        log.info("aws discovery - adding host: {}:{}", host, port);
                    }
                    try {
                        final MemcachedClient client = new MemcachedClient(null, Lists.newArrayList(new InetSocketAddress(host, Integer.parseInt(port))), false);
                        mappedNodes = client.getAvailableNodeEndPoints().stream().collect(Collectors.toMap(NodeEndPoint::getIpAddress, NodeEndPoint::getInetSocketAddress, (a, b) -> b));
                        instanceList.addAll(mappedNodes.values());
                        //instanceList.addAll(client.getAvailableNodeEndPoints().stream().map(NodeEndPoint::getInetSocketAddress).collect(Collectors.toList()));
                        client.shutdown();
                    } catch (IOException e) {
                        log.warn("Exception thrown during aws discovery memcached host registration: {}", Throwables.getStackTraceAsString(e));
                    }
                }
            }

            currentNodeList = nodeListString;
            log.info("List by Servergroup: " + instancesSpecific);
            return instancesSpecific;
        }

        return Collections.emptyMap();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"Current Node List\":\"");
        builder.append(currentNodeList);
        builder.append("\",\"System Property Name\":\"");
        builder.append(propertyName);
        builder.append("\"}");
        return builder.toString();
    }
}
