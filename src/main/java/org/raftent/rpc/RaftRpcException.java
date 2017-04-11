package org.raftent.rpc;

public class RaftRpcException extends Exception {
	private static final long serialVersionUID = 3519937283543060420L;

	public RaftRpcException(String msg) {
		super(msg);
	}

	public RaftRpcException(String msg, Exception e) {
		super(msg, e);
	}

	public RaftRpcException(Exception e) {
		super(e);
	}
}
