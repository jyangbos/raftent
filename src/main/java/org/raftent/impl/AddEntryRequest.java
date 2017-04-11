package org.raftent.impl;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

class AddEntryRequest extends RaftMessage {
	private String requestId;
	private int nodeId;
	private long timestamp;
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
	private Object data;

	public AddEntryRequest() {}

	public AddEntryRequest(String fsmId, String requestId, int nodeId, long timestamp, Object data) {
		super(fsmId);
		this.requestId = requestId;
		this.nodeId = nodeId;
		this.data = data;
		this.timestamp = timestamp;
	}

	public String getRequestId() {
		return requestId;
	}

	public int getNodeId() {
		return nodeId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Object getData() {
		return data;
	}
}
