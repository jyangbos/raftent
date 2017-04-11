package org.raftent.impl;

class VoteResponse extends RaftMessage {
	private long term;
	private int voteBy;

	public VoteResponse() {}

	public VoteResponse(String fsmId, long term, int voteBy) {
		super(fsmId);
		this.term = term;
		this.voteBy = voteBy;
	}

	public long getTerm() {
		return term;
	}

	public int getVoteBy() {
		return voteBy;
	}
}
