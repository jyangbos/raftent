package org.raftent.impl.messages;

public class AddEntryResponse extends RaftMessage {
	private String requestId;
	private boolean success;

	public AddEntryResponse() {}

	public AddEntryResponse(String fsmId, String requestId, boolean success) {
		super(fsmId);
		this.requestId = requestId;
		this.success = success;
	}

	public String getRequestId() {
		return requestId;
	}

	public boolean getSuccess() {
		return success;
	}
}
