package org.raftent.impl;

class AppendEntriesRequest extends RaftMessage {
	private long term;
	private int leaderId;
	private long prevLogIndex;
	private long prevLogTerm;
	private LogEntry[] entries;
	private long leaderCommit;

	public AppendEntriesRequest() {}

	public AppendEntriesRequest(String fsmId, long term, int leaderId, long prevLogIndex, long prevLogTerm, LogEntry[] entries, long leaderCommit) {
		super(fsmId);
		this.term = term;
		this.leaderId = leaderId;
		this.prevLogIndex = prevLogIndex;
		this.prevLogTerm = prevLogTerm;
		this.entries = entries;
		this.leaderCommit = leaderCommit;
	}

	public long getTerm() {
		return term;
	}

	public int getLeaderId() {
		return leaderId;
	}

	public long getPrevLogIndex() {
		return prevLogIndex;
	}

	public long getPrevLogTerm() {
		return prevLogTerm;
	}

	public LogEntry[] getEntries() {
		return entries;
	}

	public long getLeaderCommit() {
		return leaderCommit;
	}
}
