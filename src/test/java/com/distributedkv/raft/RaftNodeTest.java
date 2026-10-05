package com.distributedkv.raft;

import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.network.protocol.RaftMessage;
import com.distributedkv.storage.StorageEngine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the RaftNode implementation.
 */
public class RaftNodeTest {

    private RaftNode raftNode;

    @Mock
    private StateMachine stateMachine;

    @Mock
    private StorageEngine storageEngine;

    private RaftConfig raftConfig;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Create a test configuration
        raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutMinMs(150);
        raftConfig.setElectionTimeoutMaxMs(300);
        raftConfig.setHeartbeatIntervalMs(50);
        raftConfig.setLogReplicationBatchSize(100);
        raftConfig.setSnapshotThreshold(1000);

        metricsRegistry = new MetricsRegistry();

        // Configure mocks
        when(storageEngine.initialize()).thenReturn(Result.success(null));
        when(stateMachine.initialize()).thenReturn(Result.success(null));
        when(stateMachine.close()).thenReturn(Result.success(null));
        when(storageEngine.close()).thenReturn(Result.success(null));

        // Create the RaftNode
        raftNode = new RaftNode(
            "raft-1",
            "node-1",
            raftConfig,
            stateMachine,
            storageEngine,
            metricsRegistry
        );
    }

    @After
    public void tearDown() {
        if (raftNode != null) {
            raftNode.stop();
        }
    }

    @Test
    public void testInitialize() {
        // Test that the RaftNode can be initialized
        Result<Void> result = raftNode.initialize();

        assertTrue("Initialize should succeed", result.isSuccess());
        verify(stateMachine).initialize();
        verify(storageEngine).initialize();
    }

    @Test
    public void testStartAndStop() {
        // Test that the RaftNode can be started and stopped
        Result<Void> initResult = raftNode.initialize();
        assertTrue("Initialize should succeed", initResult.isSuccess());

        Result<Void> startResult = raftNode.start();
        assertTrue("Start should succeed", startResult.isSuccess());

        // Verify the node is in follower state initially
        assertEquals("Node should start as follower", RaftNode.State.FOLLOWER, raftNode.getState());

        Result<Void> stopResult = raftNode.stop();
        assertTrue("Stop should succeed", stopResult.isSuccess());

        verify(stateMachine).close();
        verify(storageEngine).close();
    }

    @Test
    public void testElectionTimeout() throws InterruptedException {
        // Test that a node starts an election when the election timeout expires
        Result<Void> initResult = raftNode.initialize();
        assertTrue("Initialize should succeed", initResult.isSuccess());

        // Configure the raft node to not actually send RPCs (to avoid complicated setup)
        raftNode = spy(raftNode);
        doReturn(CompletableFuture.completedFuture(true))
            .when(raftNode).sendVoteRequest(anyString(), any(RaftMessage.class));

        // Use a latch to detect state change to candidate
        CountDownLatch candidateLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            RaftNode.State newState = invocation.getArgument(0);
            if (newState == RaftNode.State.CANDIDATE) {
                candidateLatch.countDown();
            }
            return invocation.callRealMethod();
        }).when(raftNode).setState(any(RaftNode.State.class));

        // Start the node
        Result<Void> startResult = raftNode.start();
        assertTrue("Start should succeed", startResult.isSuccess());

        // Wait for the election timeout to trigger
        boolean becameCandidate = candidateLatch.await(2000, TimeUnit.MILLISECONDS);
        assertTrue("Node should become candidate", becameCandidate);

        // Verify that the term was incremented and votes were requested
        verify(raftNode, atLeastOnce()).setState(RaftNode.State.CANDIDATE);
        verify(raftNode, atLeastOnce()).sendVoteRequest(anyString(), any(RaftMessage.class));

        Result<Void> stopResult = raftNode.stop();
        assertTrue("Stop should succeed", stopResult.isSuccess());
    }

    @Test
    public void testHandleVoteRequest() {
        // Test that the node properly processes vote requests
        Result<Void> initResult = raftNode.initialize();
        assertTrue("Initialize should succeed", initResult.isSuccess());

        Result<Void> startResult = raftNode.start();
        assertTrue("Start should succeed", startResult.isSuccess());

        // Create a vote request
        Request request = new Request();
        request.setType(Request.Type.RAFT);
        request.setRequestId(1);

        RaftMessage raftMessage = new RaftMessage();
        raftMessage.setType(RaftMessage.Type.VOTE_REQUEST);
        raftMessage.setTerm(1); // Higher term than initial (0)
        raftMessage.setCandidateId("node-2");
        raftMessage.setLastLogIndex(0);
        raftMessage.setLastLogTerm(0);

        request.setRaftMessage(raftMessage);

        // Handle the request
        Response response = raftNode.handleRequest(request);

        // Verify the response
        assertEquals("Response should be OK", Status.OK, response.getStatus());
        assertNotNull("Response should have a Raft message", response.getRaftMessage());

        RaftMessage responseMessage = response.getRaftMessage();
        assertEquals("Response type should be VOTE_RESPONSE",
                   RaftMessage.Type.VOTE_RESPONSE, responseMessage.getType());
        assertEquals("Term should match", 1, responseMessage.getTerm());
        assertTrue("Vote should be granted", responseMessage.isVoteGranted());

        Result<Void> stopResult = raftNode.stop();
        assertTrue("Stop should succeed", stopResult.isSuccess());
    }

    @Test
    public void testHandleAppendEntriesRequest() {
        // Test that the node properly processes append entries requests
        Result<Void> initResult = raftNode.initialize();
        assertTrue("Initialize should succeed", initResult.isSuccess());

        Result<Void> startResult = raftNode.start();
        assertTrue("Start should succeed", startResult.isSuccess());

        // Create an append entries request (heartbeat)
        Request request = new Request();
        request.setType(Request.Type.RAFT);
        request.setRequestId(1);

        RaftMessage raftMessage = new RaftMessage();
        raftMessage.setType(RaftMessage.Type.APPEND_ENTRIES_REQUEST);
        raftMessage.setTerm(1); // Higher term than initial (0)
        raftMessage.setLeaderId("node-2");
        raftMessage.setPrevLogIndex(0);
        raftMessage.setPrevLogTerm(0);
        raftMessage.setLeaderCommit(0);
        // No entries, so this is a heartbeat

        request.setRaftMessage(raftMessage);

        // Handle the request
        Response response = raftNode.handleRequest(request);

        // Verify the response
        assertEquals("Response should be OK", Status.OK, response.getStatus());
        assertNotNull("Response should have a Raft message", response.getRaftMessage());

        RaftMessage responseMessage = response.getRaftMessage();
        assertEquals("Response type should be APPEND_ENTRIES_RESPONSE",
                   RaftMessage.Type.APPEND_ENTRIES_RESPONSE, responseMessage.getType());
        assertEquals("Term should match", 1, responseMessage.getTerm());
        assertTrue("Should be successful", responseMessage.isSuccess());

        // Verify the node stayed as follower and reset election timeout
        assertEquals("Node should remain follower", RaftNode.State.FOLLOWER, raftNode.getState());

        Result<Void> stopResult = raftNode.stop();
        assertTrue("Stop should succeed", stopResult.isSuccess());
    }

    @Test
    public void testLeaderElection() throws InterruptedException {
        // Test a complete leader election scenario with multiple nodes

        // Create multiple RaftNodes (just mocking the interactions)
        RaftNode raftNode1 = raftNode; // Use the one from setUp

        // Create additional mocked nodes
        RaftNode raftNode2 = mock(RaftNode.class);
        RaftNode raftNode3 = mock(RaftNode.class);

        // Configure the spy on raftNode1 to intercept RPCs
        raftNode1 = spy(raftNode1);

        // Mock the sendVoteRequest to simulate responses from other nodes
        ArgumentCaptor<RaftMessage> voteRequestCaptor = ArgumentCaptor.forClass(RaftMessage.class);

        when(raftNode1.sendVoteRequest(eq("node-2"), voteRequestCaptor.capture()))
            .thenAnswer(invocation -> {
                // Simulate a response that votes for raftNode1
                RaftMessage request = voteRequestCaptor.getValue();
                return CompletableFuture.completedFuture(true);
            });

        when(raftNode1.sendVoteRequest(eq("node-3"), voteRequestCaptor.capture()))
            .thenAnswer(invocation -> {
                // Simulate a response that votes for raftNode1
                RaftMessage request = voteRequestCaptor.getValue();
                return CompletableFuture.completedFuture(true);
            });

        // Use a latch to detect state change to leader
        CountDownLatch leaderLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            RaftNode.State newState = invocation.getArgument(0);
            if (newState == RaftNode.State.LEADER) {
                leaderLatch.countDown();
            }
            return invocation.callRealMethod();
        }).when(raftNode1).setState(any(RaftNode.State.class));

        // Configure peers
        List<String> peers = Arrays.asList("node-2", "node-3");
        raftNode1.setPeers(peers);

        // Initialize and start the node
        Result<Void> initResult = raftNode1.initialize();
        assertTrue("Initialize should succeed", initResult.isSuccess());

        // Manually trigger an election
        raftNode1.startElection();

        // Wait for the node to become leader
        boolean becameLeader = leaderLatch.await(1000, TimeUnit.MILLISECONDS);
        assertTrue("Node should become leader", becameLeader);

        // Verify that the node sent vote requests and became leader
        verify(raftNode1).sendVoteRequest(eq("node-2"), any(RaftMessage.class));
        verify(raftNode1).sendVoteRequest(eq("node-3"), any(RaftMessage.class));
        assertEquals("Node should be leader", RaftNode.State.LEADER, raftNode1.getState());

        Result<Void> stopResult = raftNode1.stop();
        assertTrue("Stop should succeed", stopResult.isSuccess());
    }

    @Test
    public void testLogReplication() {
        // Test log replication from leader to followers

        // Configure the raftNode as leader
        raftNode = spy(raftNode);
        raftNode.setCurrentTerm(1);
        raftNode.setState(RaftNode.State.LEADER);

        // Configure peers
        List<String> peers = Arrays.asList("node-2", "node-3");
        raftNode.setPeers(peers);

        // Create a log entry to replicate
        LogEntry entry = new LogEntry(1, 1, "test-data".getBytes());

        // Configure the storage engine to return successful append
        when(storageEngine.append(any(LogEntry.class))).thenReturn(Result.success(1L));

        // Configure sendAppendEntries to simulate successful responses
        ArgumentCaptor<RaftMessage> appendEntriesCaptor = ArgumentCaptor.forClass(RaftMessage.class);

        when(raftNode.sendAppendEntries(eq("node-2"), appendEntriesCaptor.capture()))
            .thenAnswer(invocation -> {
                RaftMessage request = appendEntriesCaptor.getValue();
                RaftMessage response = new RaftMessage();
                response.setType(RaftMessage.Type.APPEND_ENTRIES_RESPONSE);
                response.setTerm(request.getTerm());
                response.setSuccess(true);
                response.setLastLogIndex(request.getEntriesLastIndex());
                return CompletableFuture.completedFuture(response);
            });

        when(raftNode.sendAppendEntries(eq("node-3"), appendEntriesCaptor.capture()))
            .thenAnswer(invocation -> {
                RaftMessage request = appendEntriesCaptor.getValue();
                RaftMessage response = new RaftMessage();
                response.setType(RaftMessage.Type.APPEND_ENTRIES_RESPONSE);
                response.setTerm(request.getTerm());
                response.setSuccess(true);
                response.setLastLogIndex(request.getEntriesLastIndex());
                return CompletableFuture.completedFuture(response);
            });

        // Submit a client request
        Request request = new Request();
        request.setType(Request.Type.RAFT);
        request.setRequestId(1);

        RaftMessage raftMessage = new RaftMessage();
        raftMessage.setType(RaftMessage.Type.CLIENT_REQUEST);
        raftMessage.setData("test-data".getBytes());

        request.setRaftMessage(raftMessage);

        // Handle the request
        Response response = raftNode.handleRequest(request);

        // Verify that the entry was appended locally
        verify(storageEngine).append(any(LogEntry.class));

        // Verify that append entries were sent to followers
        verify(raftNode).sendAppendEntries(eq("node-2"), any(RaftMessage.class));
        verify(raftNode).sendAppendEntries(eq("node-3"), any(RaftMessage.class));

        // Verify the response
        assertEquals("Response should be OK", Status.OK, response.getStatus());

        // Verify that the entry was committed and applied
        verify(stateMachine).apply(any(LogEntry.class));
    }

    @Test
    public void testSnapshotCreation() {
        // Test that the node creates snapshots when the log grows too large

        // Configure mocks
        when(storageEngine.getLastLogIndex()).thenReturn(raftConfig.getSnapshotThreshold() + 1);
        when(storageEngine.getFirstLogIndex()).thenReturn(1L);
        when(stateMachine.createSnapshot()).thenReturn(Result.success(null));

        Snapshot snapshot = new Snapshot(raftConfig.getSnapshotThreshold(), 1, new byte[0]);
        when(stateMachine.getLatestSnapshot()).thenReturn(snapshot);
        when(storageEngine.deleteLogBefore(anyLong())).thenReturn(Result.success(null));

        // Initialize and start the node
        Result<Void> initResult = raftNode.initialize();
        assertTrue("Initialize should succeed", initResult.isSuccess());

        Result<Void> startResult = raftNode.start();
        assertTrue("Start should succeed", startResult.isSuccess());

        // Trigger a snapshot check (normally done periodically)
        raftNode.checkAndCreateSnapshot();

        // Verify that a snapshot was created and logs were compacted
        verify(stateMachine).createSnapshot();
        verify(storageEngine).deleteLogBefore(snapshot.getLastIncludedIndex() + 1);

        Result<Void> stopResult = raftNode.stop();
        assertTrue("Stop should succeed", stopResult.isSuccess());
    }
}
