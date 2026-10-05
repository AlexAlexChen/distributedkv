package com.distributedkv.raft;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.network.protocol.RaftMessage;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.storage.StorageEngine;

public class FollowerReadTest {

    private RaftNode leader;
    private RaftNode follower1;
    private RaftNode follower2;
    private RaftConfig config;
    private StorageEngine mockStorage;
    private StateMachine mockStateMachine;
    private RpcClient mockRpcClient;

    @Before
    public void setUp() throws Exception {
        // Create mock dependencies
        mockStorage = mock(StorageEngine.class);
        mockStateMachine = mock(StateMachine.class);
        mockRpcClient = mock(RpcClient.class);

        // Setup Raft configuration
        config = new RaftConfig();
        config.setElectionTimeoutMs(500);
        config.setHeartbeatIntervalMs(100);
        config.setFollowerReadOption(true); // Enable FollowerRead option

        // Initialize nodes with different IDs
        leader = createRaftNode("leader", Arrays.asList("leader", "follower1", "follower2"));
        follower1 = createRaftNode("follower1", Arrays.asList("leader", "follower1", "follower2"));
        follower2 = createRaftNode("follower2", Arrays.asList("leader", "follower1", "follower2"));

        // Set up leader state directly to avoid election process
        leader.updateTerm(1);
        leader.becomeLeader();
        leader.setCommitIndex(10); // Set some commit index for testing

        // Set up followers
        follower1.updateTerm(1);
        follower1.becomeFollower("leader");
        follower1.setCommitIndex(10); // Same as leader
        follower1.setAppliedIndex(10); // Same as commit index

        follower2.updateTerm(1);
        follower2.becomeFollower("leader");
        follower2.setCommitIndex(8); // Behind leader
        follower2.setAppliedIndex(8); // Same as its commit index

        // Set up RPC client to route messages appropriately during tests
        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.class)))
            .thenAnswer(invocation -> {
                RaftMessage message = invocation.getArgument(1);
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leader.handleRaftMessage(message), Status.OK()));
                return future;
            });

        when(mockRpcClient.sendMessage(eq("follower1"), any(RaftMessage.class)))
            .thenAnswer(invocation -> {
                RaftMessage message = invocation.getArgument(1);
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(follower1.handleRaftMessage(message), Status.OK()));
                return future;
            });

        when(mockRpcClient.sendMessage(eq("follower2"), any(RaftMessage.class)))
            .thenAnswer(invocation -> {
                RaftMessage message = invocation.getArgument(1);
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(follower2.handleRaftMessage(message), Status.OK()));
                return future;
            });

        // Mock the storage engine's log entries
        when(mockStorage.getLastLogIndex()).thenReturn(10L);
        when(mockStorage.getLastLogTerm()).thenReturn(1L);
    }

    private RaftNode createRaftNode(String nodeId, List<String> peerIds) {
        RaftNode node = new RaftNode(nodeId, config);
        node.setStateMachine(mockStateMachine);
        node.setStorage(mockStorage);
        node.setRpcClient(mockRpcClient);

        // Setup peers excluding the node itself
        for (String peerId : peerIds) {
            if (!peerId.equals(nodeId)) {
                node.addPeer(new RaftPeer(peerId));
            }
        }

        return node;
    }

    @After
    public void tearDown() throws Exception {
        if (leader != null) leader.shutdown();
        if (follower1 != null) follower1.shutdown();
        if (follower2 != null) follower2.shutdown();
    }

    @Test
    public void testFollowerReadBasicOperation() throws Exception {
        // Setup state machine mock to return specific value for the read
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Setup leader info message response
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            1,               // term
            10,              // leader commit index
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Follower1 performs a read
        CompletableFuture<Result<byte[]>> readFuture = follower1.followerRead("test-key".getBytes());

        // Verify follower sent LeaderInfoRequest to the leader
        verify(mockRpcClient, times(1)).sendMessage(
            eq("leader"),
            any(RaftMessage.LeaderInfoRequest.class)
        );

        // The read should complete
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        // Verify the read was successful and returned the expected value
        assertTrue("FollowerRead should succeed", result.isOk());
        assertArrayEquals("FollowerRead should return correct value", expectedValue, result.getValue());

        // Verify the follower's state machine get method was called
        verify(mockStateMachine, times(1)).get(eq("test-key".getBytes()));
    }

    @Test
    public void testFollowerReadRedirectsToLeaderWhenBehind() throws Exception {
        // Setup state machine mock to return specific value for the read
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Setup leader info message with higher commit index
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            1,               // term
            12,              // leader commit index is higher than follower2's
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Setup leader read response
        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.ReadIndexRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                RaftMessage.ReadIndexResponse response =
                    new RaftMessage.ReadIndexResponse(1, Status.OK(), expectedValue);
                future.complete(new Result<>(response, Status.OK()));
                return future;
            });

        // Follower2 is behind (commitIndex = 8, leader's = 12)
        CompletableFuture<Result<byte[]>> readFuture = follower2.followerRead("test-key".getBytes());

        // Verify follower sent LeaderInfoRequest to the leader
        verify(mockRpcClient, times(1)).sendMessage(
            eq("leader"),
            any(RaftMessage.LeaderInfoRequest.class)
        );

        // Verify follower redirected read to leader
        verify(mockRpcClient, times(1)).sendMessage(
            eq("leader"),
            any(RaftMessage.ReadIndexRequest.class)
        );

        // The read should complete
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        // Verify the read was successful and returned the expected value
        assertTrue("FollowerRead should succeed when redirected", result.isOk());
        assertArrayEquals("FollowerRead should return correct value", expectedValue, result.getValue());

        // Verify the follower's state machine was NOT called (leader handled the read)
        verify(mockStateMachine, never()).get(eq("test-key".getBytes()));
    }

    @Test
    public void testFollowerReadWaitsForApply() throws Exception {
        // Setup state machine mock to return specific value for the read
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Setup leader info message with same commit index as follower1
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            1,               // term
            10,              // leader commit index
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Temporarily set follower1's applied index behind commit index
        follower1.setAppliedIndex(8);

        // Follower1 performs a read
        CompletableFuture<Result<byte[]>> readFuture = follower1.followerRead("test-key".getBytes());

        // The future should not complete yet since applied index < commit index
        assertFalse("FollowerRead should wait for apply", readFuture.isDone());

        // Now simulate the state machine catching up
        follower1.setAppliedIndex(10);

        // The read should now complete
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        // Verify the read was successful
        assertTrue("FollowerRead should succeed after applying", result.isOk());
        assertArrayEquals("FollowerRead should return correct value", expectedValue, result.getValue());
    }

    @Test
    public void testFollowerReadHandlesLeaderChange() throws Exception {
        // Setup state machine mock
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Setup leader info message with term change
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            2,               // higher term
            10,              // leader commit index
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Follower1 performs a read
        CompletableFuture<Result<byte[]>> readFuture = follower1.followerRead("test-key".getBytes());

        // The follower should update its term when it receives the response
        assertEquals("Follower should update term from leader info", 2, follower1.getCurrentTerm());

        // The read should fail due to term change
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        assertFalse("FollowerRead should fail on term change", result.isOk());
        assertEquals("FollowerRead should report term change",
                    Status.TERM_CHANGED(), result.getStatus());
    }

    @Test
    public void testFollowerReadHandlesLeaderFailure() throws Exception {
        // Setup leader info message request to fail
        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(null, Status.CONNECTION_FAILURE()));
                return future;
            });

        // Follower1 performs a read
        CompletableFuture<Result<byte[]>> readFuture = follower1.followerRead("test-key".getBytes());

        // The read should fail due to connection failure
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        assertFalse("FollowerRead should fail on leader connection failure", result.isOk());
        assertEquals("FollowerRead should report connection failure",
                    Status.CONNECTION_FAILURE(), result.getStatus());
    }

    @Test
    public void testFollowerReadPerformance() throws Exception {
        // Setup performance test
        int numReads = 100;
        CompletableFuture<Result<byte[]>>[] futures = new CompletableFuture[numReads];

        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(any(byte[].class))).thenReturn(expectedValue);

        // Setup leader info message
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            1,               // term
            10,              // leader commit index
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Record start time
        long startTime = System.currentTimeMillis();

        // Perform multiple reads
        for (int i = 0; i < numReads; i++) {
            byte[] key = ("key-" + i).getBytes();
            futures[i] = follower1.followerRead(key);
        }

        // Wait for all reads to complete
        for (int i = 0; i < numReads; i++) {
            Result<byte[]> result = futures[i].get(1, TimeUnit.SECONDS);
            assertTrue("All reads should succeed", result.isOk());
        }

        // Record end time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // Calculate average latency
        double avgLatency = (double) totalTime / numReads;

        // Verify the average latency is within acceptable range
        assertTrue("FollowerRead average latency should be reasonable", avgLatency < 20);

        // Verify the state machine's get method was called for each read
        verify(mockStateMachine, times(numReads)).get(any(byte[].class));

        // Verify leader info was only requested once for batch of reads (caching behavior)
        verify(mockRpcClient, atMost(10)).sendMessage(
            eq("leader"),
            any(RaftMessage.LeaderInfoRequest.class)
        );
    }

    @Test
    public void testFollowerReadCachesLeaderInfo() throws Exception {
        // Setup state machine mock
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(any(byte[].class))).thenReturn(expectedValue);

        // Setup leader info message
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            1,               // term
            10,              // leader commit index
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Perform two consecutive reads
        follower1.followerRead("key1".getBytes()).get(1, TimeUnit.SECONDS);
        follower1.followerRead("key2".getBytes()).get(1, TimeUnit.SECONDS);

        // Verify leader info was only requested once due to caching
        verify(mockRpcClient, times(1)).sendMessage(
            eq("leader"),
            any(RaftMessage.LeaderInfoRequest.class)
        );
    }

    @Test
    public void testFollowerReadCacheExpiration() throws Exception {
        // Setup state machine mock
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(any(byte[].class))).thenReturn(expectedValue);

        // Configure cache to expire quickly
        config.setLeaderInfoCacheTimeMs(100); // 100ms cache validity

        // Setup leader info message
        RaftMessage.LeaderInfoResponse leaderInfoResponse = new RaftMessage.LeaderInfoResponse(
            1,               // term
            10,              // leader commit index
            System.currentTimeMillis() // current leader timestamp
        );

        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.LeaderInfoRequest.class)))
            .thenAnswer(invocation -> {
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();
                future.complete(new Result<>(leaderInfoResponse, Status.OK()));
                return future;
            });

        // Perform first read
        follower1.followerRead("key1".getBytes()).get(1, TimeUnit.SECONDS);

        // Wait for cache to expire
        Thread.sleep(150);

        // Perform second read
        follower1.followerRead("key2".getBytes()).get(1, TimeUnit.SECONDS);

        // Verify leader info was requested twice due to cache expiration
        verify(mockRpcClient, times(2)).sendMessage(
            eq("leader"),
            any(RaftMessage.LeaderInfoRequest.class)
        );
    }
}
