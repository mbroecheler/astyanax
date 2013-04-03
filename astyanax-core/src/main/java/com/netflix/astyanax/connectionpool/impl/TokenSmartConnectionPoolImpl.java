/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.connectionpool.impl;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection pool that partitions connections by the hosts which own the token
 * being operated on. When a token is not available or an operation is known to
 * span multiple tokens (such as a batch mutate or an index query) host pools
 * are picked using round robin.
 * 
 * This implementation takes an optimistic approach which is optimized for a
 * well functioning ring with all nodes up and keeps downed hosts in the
 * internal data structures.
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class TokenSmartConnectionPoolImpl<CL> extends AbstractHostPartitionConnectionPool<CL> {

    private static Logger LOG = LoggerFactory.getLogger(TokenSmartConnectionPoolImpl.class);

    private AtomicInteger roundRobinCounter = new AtomicInteger(new Random().nextInt(997));
    private static final int MAX_RR_COUNTER = Integer.MAX_VALUE/2;

    public TokenSmartConnectionPoolImpl(ConnectionPoolConfiguration configuration, ConnectionFactory<CL> factory,
                                        ConnectionPoolMonitor monitor) {
        super(configuration, factory, monitor);
    }

    @SuppressWarnings("unchecked")
    public <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(Operation<CL, R> op) throws ConnectionException {
        try {
            List<HostConnectionPool<CL>> pools;
            boolean isSorted = false;

            if (op.getPinnedHost() != null) {
                HostConnectionPool<CL> pool = hosts.get(op.getPinnedHost());
                if (pool == null) {
                    throw new NoAvailableHostsException("Host " + op.getPinnedHost() + " not active");
                }
                pools = Arrays.<HostConnectionPool<CL>> asList(pool);
            }
            else {
                //update counts
                TokenHostConnectionPoolPartition<CL> partition = topology.getPartition(op.getRowKey());
                if (partition.id()!=null) {
                    Counter c = counts.get(partition.id());
                    if (c==null) {
                        counts.putIfAbsent(partition.id(),new Counter());
                        c = counts.get(partition.id());
                    }
                    c.update();
                }

                //initialize if necessary
                if (currentPartition.get()==null) {
                    //initialize
                    synchronized (this) {
                        if (currentPartition.get()==null) {
                            try {
                                updatePools();
                            } catch (InterruptedException e) {
                                throw new RuntimeException("Interrupted while initializing",e);
                            }
                            Preconditions.checkArgument(backgroundThread==null);
                            backgroundThread = new Thread(new HostUpdater());
                            backgroundThread.start();
                        }
                    }
                }
                Preconditions.checkNotNull(currentPartition.get());

                partition = currentPartition.get();
                pools = partition.getPools();
                isSorted = partition.isSorted();
            }

            int index = roundRobinCounter.incrementAndGet();
            if (index > MAX_RR_COUNTER) {
                roundRobinCounter.set(0);
            }

            AbstractExecuteWithFailoverImpl executeWithFailover = null;
            switch (config.getHostSelectorStrategy()) {
                case ROUND_ROBIN:
                    executeWithFailover = new RoundRobinExecuteWithFailover<CL, R>(config, monitor, pools, isSorted ? 0 : index);
                    break;
                case LEAST_OUTSTANDING:
                    executeWithFailover = new LeastOutstandingExecuteWithFailover<CL, R>(config, monitor, pools);
                    break;
                default:
                    executeWithFailover = new RoundRobinExecuteWithFailover<CL, R>(config, monitor, pools, isSorted ? 0 : index);
                    break;

            }
            return executeWithFailover;
        }
        catch (ConnectionException e) {
            monitor.incOperationFailure(e.getHost(), e);
            throw e;
        }
    }

    @Override
    public void shutdown() {
        while (backgroundThread.isAlive()) {
            backgroundThread.interrupt();
        }
        super.shutdown();
    }


    private static final double DECAY_EXPONENT_MULTI = 0.0005;
    private static final int BG_THREAD_WAIT_TIME = 200;
    private static final int MAX_CLOSE_ATTEMPTS = 5;
    private static final int DEFAULT_UPDATE_INTERVAL = 4000+BG_THREAD_WAIT_TIME*MAX_CLOSE_ATTEMPTS;
    private static final Random random = new Random();


    private final AtomicReference<TokenHostConnectionPoolPartition<CL>> currentPartition=new AtomicReference<TokenHostConnectionPoolPartition<CL>>();

    private Thread backgroundThread=null;
    private NonBlockingHashMap<BigInteger,Counter> counts = new NonBlockingHashMap<BigInteger,Counter>();


    private void updatePools() throws InterruptedException {
        BigInteger bestToken = null;
        double bestTokenValue = 0.0;
        for (Map.Entry<BigInteger,Counter> entry : counts.entrySet()) {
            if (bestToken==null || bestTokenValue<entry.getValue().currentValue()) {
                bestToken=entry.getKey();
                bestTokenValue=entry.getValue().currentValue();
            }
        }
        TokenHostConnectionPoolPartition<CL> nextPartition, oldPartition = currentPartition.get();
        if (bestToken==null) {
            LOG.info("No best token, using getAllPools()");
            nextPartition = topology.getAllPools();
        } else {
            nextPartition = topology.getPartition(bestToken);
        }
        Preconditions.checkNotNull(nextPartition);
        LOG.info("Updating pool to token: {}",nextPartition.id());
        currentPartition.set(nextPartition);

        //Release connections from old pool
        if (oldPartition!=null && !Objects.equal(nextPartition.id(),oldPartition.id()) ) {
            int activeConnections; int attempts=0;
            do {
                try {
                    Thread.sleep(BG_THREAD_WAIT_TIME);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting to close down old pool");
                    throw e;
                }
                attempts++;
                activeConnections = 0;
                for (HostConnectionPool<CL> pool : oldPartition.pools) {
                    pool.discardIdleConnections();
                    activeConnections += pool.getActiveConnectionCount();
                }
                LOG.info("Active connections on attempt {}: {}. Sleeping if >0",attempts,activeConnections);
            } while (activeConnections>0 && attempts<MAX_CLOSE_ATTEMPTS);
            if (attempts>=MAX_CLOSE_ATTEMPTS) LOG.info("Open connections after {} attempts: {}. Giving up.",attempts,activeConnections);
        }

    }

    private class HostUpdater implements Runnable {

        private long lastUpdateTime;
        private final long updateInterval;

        public HostUpdater() {
            this(DEFAULT_UPDATE_INTERVAL);
        }

        public HostUpdater(final long updateInterval) {
            Preconditions.checkArgument(updateInterval>0);
            this.updateInterval=updateInterval;
            lastUpdateTime = System.currentTimeMillis();
        }

        @Override
        public void run() {
            while (true) {
                long sleepTime = updateInterval - (System.currentTimeMillis()-lastUpdateTime);
                try {
                    Thread.sleep(Math.max(0,sleepTime));
                    updatePools();
                    lastUpdateTime=System.currentTimeMillis();
                } catch (InterruptedException e) {
                    LOG.info("Background update thread shutting down...");
                    return;
                }
            }
        }
    }


    private static class Counter {

        private double value=0.0;
        private long lastUpdate=0;

        public synchronized void update() {
            value = currentValue()+1.0;
            lastUpdate=System.currentTimeMillis();
        }

        public synchronized double currentValue() {
            return value*Math.exp(-DECAY_EXPONENT_MULTI*(System.currentTimeMillis()-lastUpdate));
        }

    }

}
