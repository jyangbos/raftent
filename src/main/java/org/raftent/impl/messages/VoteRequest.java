package org.raftent.impl.messages;

public class VoteRequest extends RaftMessage {
	private long term;
	private int candidateId;
	private long lastLogIndex;
	private long lastLogTerm;

	public VoteRequest() {}

	public VoteRequest(String fsmId, long term, int candidateId, long lastLogIndex, long lastLogTerm) {
		super(fsmId);
		this.term = term;
		this.candidateId = candidateId;
		this.lastLogIndex = lastLogIndex;
		this.lastLogTerm = lastLogTerm;
	}

	public long getTerm() {
		return term;
	}

	public int getCandidateId() {
		return candidateId;
	}

	public long getLastLogIndex() {
		return lastLogIndex;
	}

	public long getLastLogTerm() {
		return lastLogTerm;
	}
}
