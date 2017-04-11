package org.raftent.rpc;

public interface Receiver {
	void handleRequest();
	void terminate();
}
