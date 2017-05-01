package org.raftent.impl.messages;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class LogEntry {
	private long termId;
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
	private Object entry;

	public LogEntry() {}

	public LogEntry(long currentTerm, Object entry) {
		this.termId = currentTerm;
		this.entry = entry;
	}

	public long getTermId() {
		return termId;
	}

	public Object getEntry() {
		return entry;
	}
}
