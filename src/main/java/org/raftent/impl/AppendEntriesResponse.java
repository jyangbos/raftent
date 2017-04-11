package org.raftent.impl;

class AppendEntriesResponse extends RaftMessage {
	private long term;
	private boolean success;
	private int id;
	private long lastLogIndex;

	public AppendEntriesResponse() {}

	public AppendEntriesResponse(String fsmId, int id, long term, long lastLogIndex, boolean success) {
		super(fsmId);
		this.id = id;
		this.term = term;
		this.lastLogIndex = lastLogIndex;
		this.success = success;
	}

	public int getId() {
		return id;
	}

	public long getTerm() {
		return term;
	}

	public long getLastLogIndex() {
		return lastLogIndex;
	}

	public boolean getSuccess() {
		return success;
	}
}
