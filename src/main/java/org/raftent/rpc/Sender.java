package org.raftent.rpc;

public interface Sender {
	void send(Object data) throws RaftRpcException;
	void terminate();
}
