package org.raftent.impl.messages;

public class RaftMessage {
	private String fsmId;

	public RaftMessage() {}

	public RaftMessage(String fsmId) {
		this.fsmId = fsmId;
	}

	public String getFsmId() {
		return fsmId;
	}
}
