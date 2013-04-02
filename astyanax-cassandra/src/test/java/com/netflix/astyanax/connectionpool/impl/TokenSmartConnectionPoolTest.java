package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.partitioner.OrderedBigIntegerPartitioner;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.test.*;
import com.netflix.astyanax.util.TokenGenerator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TokenSmartConnectionPoolTest extends BaseConnectionPoolTest {
    private static Logger LOG = LoggerFactory.getLogger(TokenSmartConnectionPoolTest.class);
    private static Operation<TestClient, String> dummyOperation = new TestOperation();

    protected ConnectionPool<TestClient> createPool() {
        ConnectionPoolConfiguration config = new ConnectionPoolConfigurationImpl(
                TestConstants.CLUSTER_NAME + "_" + TestConstants.KEYSPACE_NAME)
            .setPartitioner(OrderedBigIntegerPartitioner.get());
        config.initialize();

        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();

        return new TokenSmartConnectionPoolImpl<TestClient>(
                config, new TestConnectionFactory(config, monitor), monitor);
    }

    @Test
    public void testTokenMappingForMidRangeTokens() throws ConnectionException {
        ConnectionPool<TestClient> cp = createPool();

        List<Host> ring1 = makeRing(3, 1, 1);
        LOG.info("testTokenMappingForMidRangeTokens\n" + TestTokenRange.getRingDetails(ring1));
        
        cp.setHosts(ring1);
        
        BigInteger threeNodeRingIncrement = TokenGenerator.MAXIMUM.divide(new BigInteger("3"));

        RetryPolicy retryPolicy = new RunOnce();

        BigInteger key = BigInteger.ZERO;
        LOG.info(key.toString() + " 127.0.1.2");
        OperationResult<String> result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        key = BigInteger.ONE;
        LOG.info(key.toString() + " 127.0.1.0");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        key = threeNodeRingIncrement.subtract(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.0");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());
        
        key = threeNodeRingIncrement;
        LOG.info(key.toString() + " 127.0.1.0");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());
        
        key = threeNodeRingIncrement.add(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.1");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());
        
        key = threeNodeRingIncrement.add(threeNodeRingIncrement).add(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.1");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        key = threeNodeRingIncrement.add(threeNodeRingIncrement).add(BigInteger.ONE).add(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.2");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        cp.shutdown();
    }

    @Test
    public void testTokenUpdate() throws ConnectionException, InterruptedException {
        ConnectionPool<TestClient> cp = createPool();

        List<Host> ring1 = makeRing(3, 1, 1);
        LOG.info("testTokenUpdate\n" + TestTokenRange.getRingDetails(ring1));

        cp.setHosts(ring1);

        BigInteger threeNodeRingIncrement = TokenGenerator.MAXIMUM.divide(new BigInteger("3"));

        RetryPolicy retryPolicy = new RunOnce();

        BigInteger key = BigInteger.ZERO;
        LOG.info(key.toString() + " 127.0.1.2");
        OperationResult<String> result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        key = BigInteger.ONE;
        LOG.info(key.toString() + " 127.0.1.0");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        key = threeNodeRingIncrement.subtract(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.0");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        key = threeNodeRingIncrement;
        LOG.info(key.toString() + " 127.0.1.0");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        Thread.sleep(6000);

        key = threeNodeRingIncrement.add(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.1");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.0",result.getHost().getIpAddress());

        key = threeNodeRingIncrement.add(threeNodeRingIncrement).add(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.1");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.0",result.getHost().getIpAddress());

        key = threeNodeRingIncrement.add(threeNodeRingIncrement).add(BigInteger.ONE).add(BigInteger.ONE);
        LOG.info(key.toString() + " 127.0.1.2");
        result = cp.executeWithFailover(new TokenTestOperation(key), retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.0",result.getHost().getIpAddress());

        cp.shutdown();
    }

    private List<Host> makeRing(int nHosts, int replication_factor, int id) {
        return TestTokenRange.makeRing(nHosts,replication_factor,id,TokenGenerator.MINIMUM,TokenGenerator.MAXIMUM);
    }
}
