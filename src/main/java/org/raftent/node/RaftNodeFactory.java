package org.raftent.node;

import org.raftent.rpc.Messager;

public class RaftNodeFactory {
	private String[] nodes;
	private int id;
	private String logFilePrefix;
	private Messager messager;

	public static RaftNodeFactory newInstance(String[] nodes, int id, String logFilePrefix) {
		return new RaftNodeFactory(nodes, id, logFilePrefix, null);
	}

	public static RaftNodeFactory newInstance(String[] nodes, int id, String logFilePrefix, Messager messager) {
		return new RaftNodeFactory(nodes, id, logFilePrefix, messager);
	}

	private RaftNodeFactory(String[] nodes, int id, String logFilePrefix, Messager messager) {
		this.nodes = nodes;
		this.id = id;
		this.logFilePrefix = logFilePrefix;
		this.messager = messager;
	}

	public RaftNode newRaftNode() throws RaftNodeException {
		return new RaftNode(nodes, id, logFilePrefix, messager);
	}
}
