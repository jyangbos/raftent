package org.raftent.rpc;

public interface MessageHandler {
	void handle(Object object) throws RaftRpcException;
}
