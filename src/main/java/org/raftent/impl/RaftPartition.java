package org.raftent.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.raftent.node.LogProposal;
import org.raftent.node.RaftNodeException;
import org.raftent.node.StateMachine;
import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftPartition implements LogProposal {
    private static final Logger logger = LoggerFactory.getLogger(RaftPartition.class);
	// persistent
	private long currentTerm;
	private int votedFor;
	private long lastLogIndex;
	// transient
	private long commitIndex;
	private long lastApplied;
	private long lastPing;
	private long nextElection;
	
	private Set<Integer> voteCount;
	private int leaderId;
	// for leaders
	private long[] nextIndex;
	private long[] matchIndex;

	// for local
	private volatile NodeState state;
	private final int id;
	private final Sender[] senders;
	private final StateMachine fsm;
	private final String fsmId;
	private final Map<Long, AddEntryRequest> pendingCommits;
	private final Map<String, FuturePost> postFutures;
	private final PersistentStore dataStore;
	private final ObjectDataConverter converter;
	private final Random rand;
	private final int inactivityTimeout;
	private final int pingTimeout;

	public RaftPartition(int id, Sender[] senders, String logFilePrefix, StateMachine fsm, ObjectDataConverter converter, int inactivityTimeout) throws RaftNodeException {
		this.fsmId = fsm.getName();
		this.senders = senders;
		rand = new Random(System.currentTimeMillis() + id);
		this.converter = converter;
		this.fsm = fsm;
		this.inactivityTimeout = inactivityTimeout;
		pingTimeout = inactivityTimeout >> 2;
		//
		state = NodeState.FOLLOWER;
		this.id = id;
		lastPing = System.currentTimeMillis();
		nextElection = lastPing;
		leaderId = -1;
		pendingCommits = new ConcurrentHashMap<>();
		postFutures = new ConcurrentHashMap<>();
		//
		commitIndex = 0;
		lastApplied = 0;
		nextIndex = new long[senders.length];
		matchIndex = new long[senders.length];
		//
		logFilePrefix = fsm != null ? String.format("%s_%s", logFilePrefix, fsm.getName()) : logFilePrefix;
		dataStore = new PersistentStore(String.format("%s_%d", logFilePrefix, id));
		currentTerm = dataStore.getCurrentTerm();
		votedFor = dataStore.getVotedFor();
		lastLogIndex = dataStore.getLastLogIndex();
	}

	private AppendEntriesRequest prepareAppendEntriesRequest(long term, long nextIndex) throws RaftRpcException {
		long preIndex = nextIndex - 1;
		LogEntry[] sub = new LogEntry[(int)(lastLogIndex - preIndex)];
		for (int j = 0; j < sub.length; j++) {
			sub[j] = getLogEntry(nextIndex + j);
		}
		LogEntry preEntry = getLogEntry(preIndex);
		return new AppendEntriesRequest(fsmId, term, id, preIndex, preEntry == null ? 0 : preEntry.getTermId(), sub, commitIndex);
	}

	private LogEntry getLogEntry(long index) throws RaftRpcException {
		if (index == 0 || index > dataStore.getLastLogIndex()) {
			return null;
		}
		return (LogEntry) converter.toObject(dataStore.getLogEntry(index));
	}

	private void setLogEntry(long index, LogEntry entry) throws RaftRpcException {
		byte[] data = converter.toBytes(entry);
		dataStore.setLogEntry(index, data);
	}

	private void ping() throws RaftRpcException {
		AppendEntriesRequest pingReq = null;
		for (int i = 0; i < senders.length; i++) {
			if (i != id) {
				if (nextIndex[i] < lastLogIndex) {
					AppendEntriesRequest replicationReq = prepareAppendEntriesRequest(currentTerm, nextIndex[i]);
					senders[i].send(replicationReq);
				}
				else {
					if (pingReq == null) {
						pingReq = new AppendEntriesRequest(fsmId, currentTerm, id, -1, -1, null, commitIndex);
					}
					senders[i].send(pingReq);
				}
			}
		}
		lastPing = System.currentTimeMillis();
	}

	private void handleCommitUpdate() throws RaftRpcException {
		for (long i = lastApplied + 1; i <= commitIndex; i++) {
			if (fsm != null) {
				fsm.transitState(getLogEntry(i).getEntry());
			}
		}
		lastApplied = commitIndex;
		Iterator<Map.Entry<Long, AddEntryRequest>> iter = pendingCommits.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<Long, AddEntryRequest> item = iter.next();
			if (item.getKey() <= commitIndex) {
				AddEntryRequest req = item.getValue();
				senders[req.getNodeId()].send(new AddEntryResponse(fsmId, req.getRequestId(), true));
				iter.remove();
			}
		}
	}

	private long nextElectionTime() {
		return System.currentTimeMillis() + 100 + rand.nextInt(inactivityTimeout);
	}

	public void handleTimeout() throws RaftRpcException {
		long now = System.currentTimeMillis();
		if (now >= (lastPing + pingTimeout) && state == NodeState.LEADER) {
			ping();
		}
		else if (state != NodeState.LEADER && now >= (lastPing + inactivityTimeout) && now >= nextElection) {
			state = NodeState.CANDIDATE;
			long lastIndex = lastLogIndex;
			currentTerm += 1;
			dataStore.setCurrentTerm(currentTerm);
			VoteRequest reqVote = new VoteRequest(fsmId, currentTerm, id, lastIndex, lastIndex == 0 ? 0 : getLogEntry(lastIndex).getTermId());
			voteCount = new HashSet<>();
			voteCount.add(id);
			for (int i = 0; i < senders.length; i++) {
				if (i != id) {
					senders[i].send(reqVote);
				}
			}
			nextElection = nextElectionTime();
		}
	}

	public void handleRaftMessage(Object object) throws RaftRpcException {
		if (object instanceof AddEntryRequest) {
			handleAddEntryRequest((AddEntryRequest)object);
		}
		else if (object instanceof AddEntryResponse) {
			handleAddEntryResponse((AddEntryResponse)object);
		}
		else if (object instanceof AppendEntriesRequest) {
			handleAppendEntriesRequest((AppendEntriesRequest)object);
		}
		else if (object instanceof AppendEntriesResponse) {
			handleAppendEntriesResponse((AppendEntriesResponse)object);
		}
		else if (object instanceof VoteRequest) {
			handleVoteRequest((VoteRequest)object);
		}
		else if (object instanceof VoteResponse) {
			handleVoteResponse((VoteResponse)object);
		}

	}

	private void handleAddEntryRequest(AddEntryRequest req) {
		try {
			if (state == NodeState.LEADER) {
				lastLogIndex += 1;
				setLogEntry(lastLogIndex, new LogEntry(currentTerm, req.getData()));
				dataStore.setLastLogIndex(lastLogIndex);
				pendingCommits.put(lastLogIndex, req);
				for (int i = 0; i < senders.length; i++) {
					if (i != id) {
						AppendEntriesRequest replicationReq = prepareAppendEntriesRequest(currentTerm, nextIndex[i]);
						senders[i].send(replicationReq);
					}
				}
				return;
			}
		} catch (RaftRpcException e) {
			logger.debug("Failed to send a response to AddEntryRequest", e);
		}
		try {
			senders[req.getNodeId()].send(new AddEntryResponse(fsmId, req.getRequestId(), false));
		} catch (RaftRpcException e) {
			logger.debug("Failed to respond to AddEntryRequest", e);
		}
	}

	private void handleAddEntryResponse(AddEntryResponse resp) {
		FuturePost future = postFutures.get(resp.getRequestId());
		Lock lock = future.getLock();
		Condition cond = future.getCondition();
		lock.lock();
		try {
			future.setSuccess(resp.getSuccess());
			cond.signal();
			postFutures.remove(resp.getRequestId());
		} finally {
			lock.unlock();
		}
	}

	private void handleAppendEntriesRequest(AppendEntriesRequest req) throws RaftRpcException {
		lastPing = System.currentTimeMillis();
		long reqTerm = req.getTerm();
		if (currentTerm < reqTerm) {
			currentTerm = reqTerm;
			dataStore.setCurrentTerm(currentTerm);
			state = NodeState.FOLLOWER;
		}
		else if (reqTerm == currentTerm && state == NodeState.CANDIDATE) {
			state = NodeState.FOLLOWER;
		}
		leaderId = req.getLeaderId();
		if (req.getEntries() == null) {
			return;
		}
		if (state == NodeState.FOLLOWER) {
			if (reqTerm < currentTerm) {
				senders[req.getLeaderId()].send(new AppendEntriesResponse(fsmId, id, currentTerm, lastLogIndex,  false));
				return;
			}
			else {
				long preIndex = req.getPrevLogIndex();
				LogEntry preEntry = getLogEntry(preIndex);
				if (preIndex > 0 && (preEntry == null || preEntry.getTermId() != req.getPrevLogTerm())) {
					senders[req.getLeaderId()].send(new AppendEntriesResponse(fsmId, id, currentTerm, lastLogIndex, false));
					return;
				}
				LogEntry[] entries = req.getEntries();
				long index = preIndex;
				for (int i = 0; i < entries.length; i++) {
					index++;
					setLogEntry(index, entries[i]);
				}
				lastLogIndex = index;
				dataStore.setLastLogIndex(lastLogIndex);
				long leaderCommit = req.getLeaderCommit();
				if (leaderCommit > commitIndex) {
					commitIndex = leaderCommit < index ? leaderCommit : index;
					handleCommitUpdate();
				}
				senders[req.getLeaderId()].send(new AppendEntriesResponse(fsmId, id, currentTerm, lastLogIndex, true));
				return;
			}
		}
	}

	private void handleAppendEntriesResponse(AppendEntriesResponse resp) throws RaftRpcException {
		long respTerm = resp.getTerm();
		if (currentTerm < respTerm) {
			currentTerm = respTerm;
			dataStore.setCurrentTerm(currentTerm);
			state = NodeState.FOLLOWER;
		}
		if (state == NodeState.LEADER) {
			int followerId = resp.getId();
			if (resp.getSuccess()) {
				matchIndex[followerId] = resp.getLastLogIndex();
				nextIndex[followerId] = matchIndex[followerId] + 1;
				matchIndex[id] = lastLogIndex;
				long[] temp = matchIndex.clone();
				Arrays.sort(temp);
				int i = temp.length - 1;
				for (; i >= temp.length/2; i--) {
					LogEntry entry = getLogEntry(temp[i]);
					if (currentTerm != entry.getTermId()) {
						break;
					}
				}
				if (i < (temp.length/2) && commitIndex < temp[i+1]) {
					commitIndex = temp[i+1];
					handleCommitUpdate();
				}
			}
			else {
				nextIndex[followerId] += -1;
				if (nextIndex[followerId] == 0) {
					nextIndex[followerId] = 1;
				}
				AppendEntriesRequest replicationReq = prepareAppendEntriesRequest(currentTerm, nextIndex[followerId]);
				senders[followerId].send(replicationReq);
			}
		}
	}

	private void handleVoteRequest(VoteRequest req) throws RaftRpcException {
		long reqTerm = req.getTerm();
		if (reqTerm < currentTerm) {
			senders[req.getCandidateId()].send(new VoteResponse(fsmId, currentTerm, id - senders.length));
			return;
		}
		if (currentTerm < reqTerm) {
			currentTerm = reqTerm;
			dataStore.setCurrentTerm(currentTerm);
			state = NodeState.FOLLOWER;
		}
		votedFor = dataStore.getVotedFor();
		if (state != NodeState.CANDIDATE && (votedFor == -1 || votedFor == req.getCandidateId())) {
			if (req.getLastLogIndex() >= lastLogIndex && req.getLastLogTerm() >= (lastLogIndex == 0 ? 0 : getLogEntry(lastLogIndex).getTermId())) {
				senders[req.getCandidateId()].send(new VoteResponse(fsmId, currentTerm, id));
				votedFor = req.getCandidateId();
				dataStore.setVotedFor(votedFor);
				// prevent from initiating the vote process too quickly.
				nextElection = nextElectionTime();
				return;
			}
		}
		senders[req.getCandidateId()].send(new VoteResponse(fsmId, currentTerm, id - senders.length));
	}

	private void handleVoteResponse(VoteResponse resp) throws RaftRpcException {
		long respTerm = resp.getTerm();
		if (currentTerm < respTerm) {
			currentTerm = respTerm;
			dataStore.setCurrentTerm(currentTerm);
			state = NodeState.FOLLOWER;
		}
		else {
			if (respTerm == currentTerm) {
				int voter = resp.getVoteBy();
				if (voter < 0) {
					return;
				}
				if (state != NodeState.LEADER) {
					voteCount.add(voter);
					if (voteCount.size() > (senders.length/2)) {
						state = NodeState.LEADER;
						leaderId = id;
						for (int i = 0; i < nextIndex.length; i++) {
							nextIndex[i] = lastLogIndex + 1;
							matchIndex[i] = 0;
						}
						logger.debug("Elected leader {}", leaderId);
					}
				}
				if (state == NodeState.LEADER) {
					ping();
				}
			}
		}
	}

	@Override
	public Future<Boolean> post(Object entry) throws RaftRpcException {
		if (leaderId != -1) {
			AddEntryRequest req = new AddEntryRequest(fsmId, UUID.randomUUID().toString(), id, System.currentTimeMillis(), entry);
			senders[leaderId].send(req);
			FuturePost future = new FuturePost();
			postFutures.put(req.getRequestId(), future);
			return future;
		}
		return null;
	}
}
