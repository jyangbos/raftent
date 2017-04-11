package org.raftent.impl;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

class LogEntry {
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
