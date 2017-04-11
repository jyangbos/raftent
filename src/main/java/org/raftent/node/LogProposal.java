package org.raftent.node;

import java.util.concurrent.Future;

import org.raftent.rpc.RaftRpcException;

public interface LogProposal {
	Future<Boolean> post(Object entry) throws RaftRpcException;
}
