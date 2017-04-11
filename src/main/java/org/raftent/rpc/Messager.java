package org.raftent.rpc;

public interface Messager {
	Sender getSender(String destination, int port) throws RaftRpcException;

	Receiver getReceiver() throws RaftRpcException;

	ObjectDataConverter getObjectDataConverter();
}
