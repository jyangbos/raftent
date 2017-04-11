package org.raftent.node;

public class RaftNodeException extends Exception {
	private static final long serialVersionUID = -4713037829564309370L;

	public RaftNodeException(String msg) {
		super(msg);
	}

	public RaftNodeException(String msg, Exception e) {
		super(msg, e);
	}

	public RaftNodeException(Exception e) {
		super(e);
	}
}
