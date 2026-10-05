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

public class ReadIndexTest {

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
        config.setReadIndexOption(true); // Enable ReadIndex option

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
        follower1.setCommitIndex(9); // Slightly behind the leader

        follower2.updateTerm(1);
        follower2.becomeFollower("leader");
        follower2.setCommitIndex(10); // Same as leader

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
    public void testLeaderReadIndex() throws Exception {
        // Setup state machine mock to return specific value for the read
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Leader performs ReadIndex operation
        CompletableFuture<Result<byte[]>> readFuture = leader.readIndex("test-key".getBytes());

        // Leader should first send heartbeats to confirm its leadership
        ArgumentCaptor<RaftMessage> messageCaptor = ArgumentCaptor.forClass(RaftMessage.class);
        verify(mockRpcClient, atLeast(2)).sendMessage(anyString(), messageCaptor.capture());

        // Filter captured messages to find heartbeats
        List<RaftMessage> heartbeats = messageCaptor.getAllValues().stream()
            .filter(m -> m instanceof RaftMessage.AppendEntries)
            .map(m -> (RaftMessage.AppendEntries) m)
            .filter(ae -> ae.getEntries().length == 0) // Heartbeats have no entries
            .toList();

        assertFalse("Leader should send heartbeats for ReadIndex", heartbeats.isEmpty());

        // Simulate responses from followers to confirm leadership
        for (RaftMessage heartbeat : heartbeats) {
            RaftMessage.AppendEntriesResponse response =
                new RaftMessage.AppendEntriesResponse(
                    heartbeat.getTerm(),
                    true,
                    leader.getCommitIndex()
                );
            leader.handleRaftMessage(response);
        }

        // Now the read should be served
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        // Verify the read was successful and returned the expected value
        assertTrue("ReadIndex should succeed", result.isOk());
        assertArrayEquals("ReadIndex should return correct value", expectedValue, result.getValue());

        // Verify the state machine's get method was called with the correct key
        verify(mockStateMachine, times(1)).get(eq("test-key".getBytes()));
    }

    @Test
    public void testReadIndexQuorumRequirement() throws Exception {
        // Setup state machine mock to return specific value for the read
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Leader performs ReadIndex operation
        CompletableFuture<Result<byte[]>> readFuture = leader.readIndex("test-key".getBytes());

        // Leader sends heartbeats but only one follower responds
        ArgumentCaptor<RaftMessage> messageCaptor = ArgumentCaptor.forClass(RaftMessage.class);
        verify(mockRpcClient, atLeast(2)).sendMessage(anyString(), messageCaptor.capture());

        // Simulate response from only one follower
        List<RaftMessage> heartbeats = messageCaptor.getAllValues().stream()
            .filter(m -> m instanceof RaftMessage.AppendEntries)
            .map(m -> (RaftMessage.AppendEntries) m)
            .filter(ae -> ae.getEntries().length == 0)
            .toList();

        // Only respond from follower1
        RaftMessage.AppendEntriesResponse response =
            new RaftMessage.AppendEntriesResponse(
                heartbeats.get(0).getTerm(),
                true,
                follower1.getCommitIndex()
            );
        leader.handleRaftMessage(response);

        // The future should not complete yet since we haven't reached majority
        assertFalse("ReadIndex should not complete without quorum", readFuture.isDone());

        // Now respond from follower2 as well to achieve quorum
        response = new RaftMessage.AppendEntriesResponse(
            heartbeats.get(1).getTerm(),
            true,
            follower2.getCommitIndex()
        );
        leader.handleRaftMessage(response);

        // Now the read should complete
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        // Verify the read was successful and returned the expected value
        assertTrue("ReadIndex should succeed with quorum", result.isOk());
        assertArrayEquals("ReadIndex should return correct value", expectedValue, result.getValue());
    }

    @Test
    public void testReadIndexFailsWhenLeadershipLost() throws Exception {
        // Setup state machine mock
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Leader performs ReadIndex operation
        CompletableFuture<Result<byte[]>> readFuture = leader.readIndex("test-key".getBytes());

        // Leader sends heartbeats
        ArgumentCaptor<RaftMessage> messageCaptor = ArgumentCaptor.forClass(RaftMessage.class);
        verify(mockRpcClient, atLeast(2)).sendMessage(anyString(), messageCaptor.capture());

        // But before getting responses, a higher term message arrives
        RaftMessage.AppendEntries higherTermMessage =
            new RaftMessage.AppendEntries(2, "new-leader", 10, 1, new LogEntry[0], 10);

        leader.handleRaftMessage(higherTermMessage);

        // The leader should step down
        assertEquals(RaftNode.NodeState.FOLLOWER, leader.getState());
        assertEquals(2, leader.getCurrentTerm());

        // The ReadIndex future should complete with an error
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        assertFalse("ReadIndex should fail when leadership is lost", result.isOk());
        assertEquals("ReadIndex should report leadership loss",
                    Status.NOT_LEADER(), result.getStatus());
    }

    @Test
    public void testReadIndexWaitsForCommitIndexAdvancement() throws Exception {
        // Setup state machine mock
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Leader initially has commitIndex = 10
        assertEquals(10, leader.getCommitIndex());

        // Leader receives a new entry with higher index but hasn't committed it yet
        when(mockStorage.getLastLogIndex()).thenReturn(15L);

        // Leader performs ReadIndex operation
        CompletableFuture<Result<byte[]>> readFuture = leader.readIndex("test-key".getBytes());

        // Leadership confirmation happens
        ArgumentCaptor<RaftMessage> messageCaptor = ArgumentCaptor.forClass(RaftMessage.class);
        verify(mockRpcClient, atLeast(2)).sendMessage(anyString(), messageCaptor.capture());

        // Simulate responses from followers
        List<RaftMessage> heartbeats = messageCaptor.getAllValues().stream()
            .filter(m -> m instanceof RaftMessage.AppendEntries)
            .map(m -> (RaftMessage.AppendEntries) m)
            .filter(ae -> ae.getEntries().length == 0)
            .toList();

        for (RaftMessage heartbeat : heartbeats) {
            RaftMessage.AppendEntriesResponse response =
                new RaftMessage.AppendEntriesResponse(
                    heartbeat.getTerm(),
                    true,
                    leader.getCommitIndex()
                );
            leader.handleRaftMessage(response);
        }

        // ReadIndex should be waiting for commit index to advance to at least the read index
        assertFalse("ReadIndex should wait for commit index advancement", readFuture.isDone());

        // Now simulate commit index advancement
        leader.setCommitIndex(15);

        // The read should now complete
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        // Verify the read was successful
        assertTrue("ReadIndex should succeed after commit index advances", result.isOk());
        assertArrayEquals("ReadIndex should return correct value", expectedValue, result.getValue());
    }

    @Test
    public void testFollowerRedirectsReadIndexToLeader() throws Exception {
        // Setup expected result from leader
        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(eq("test-key".getBytes()))).thenReturn(expectedValue);

        // Mock the leader's ReadIndex response
        when(mockRpcClient.sendMessage(eq("leader"), any(RaftMessage.ReadIndexRequest.class)))
            .thenAnswer(invocation -> {
                RaftMessage.ReadIndexRequest request = invocation.getArgument(1);
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();

                // Simulate leader processing the ReadIndex
                RaftMessage.ReadIndexResponse response =
                    new RaftMessage.ReadIndexResponse(1, Status.OK(), expectedValue);

                future.complete(new Result<>(response, Status.OK()));
                return future;
            });

        // Follower performs ReadIndex operation
        CompletableFuture<Result<byte[]>> readFuture = follower1.readIndex("test-key".getBytes());

        // Verify follower forwarded the request to the leader
        verify(mockRpcClient, times(1)).sendMessage(
            eq("leader"),
            any(RaftMessage.ReadIndexRequest.class)
        );

        // The read should complete successfully with the leader's response
        Result<byte[]> result = readFuture.get(1, TimeUnit.SECONDS);

        assertTrue("Follower's ReadIndex should succeed", result.isOk());
        assertArrayEquals("Follower should return leader's value", expectedValue, result.getValue());
    }

    @Test
    public void testReadIndexPerformance() throws Exception {
        // Setup performance test
        int numReads = 100;
        CompletableFuture<Result<byte[]>>[] futures = new CompletableFuture[numReads];

        byte[] expectedValue = "test-value".getBytes();
        when(mockStateMachine.get(any(byte[].class))).thenReturn(expectedValue);

        // Prepare the ReadIndex environment
        // Simulate leadership confirmation
        when(mockRpcClient.sendMessage(anyString(), any(RaftMessage.AppendEntries.class)))
            .thenAnswer(invocation -> {
                RaftMessage.AppendEntries heartbeat = invocation.getArgument(1);
                CompletableFuture<Result<RaftMessage>> future = new CompletableFuture<>();

                RaftMessage.AppendEntriesResponse response =
                    new RaftMessage.AppendEntriesResponse(
                        heartbeat.getTerm(),
                        true,
                        leader.getCommitIndex()
                    );

                future.complete(new Result<>(response, Status.OK()));
                return future;
            });

        // Record start time
        long startTime = System.currentTimeMillis();

        // Perform multiple reads
        for (int i = 0; i < numReads; i++) {
            byte[] key = ("key-" + i).getBytes();
