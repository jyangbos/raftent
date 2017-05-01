package org.raftent.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.raftent.impl.RaftPartition;
import org.raftent.impl.messages.RaftMessage;
import org.raftent.rpc.MessageHandler;
import org.raftent.rpc.Messager;
import org.raftent.rpc.MessagerFactory;
import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Receiver;
import org.raftent.rpc.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNode {
	private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
	private static final int INACTIVITY_TIMEOUT = 5000;
	private static final int RECEIVE_TIMEOUT = INACTIVITY_TIMEOUT >> 2;
	private int id;
	private String logFilePrefix;
	private ObjectDataConverter converter;
	private Sender[] senders;
	private Receiver receiver;
	private Thread messageProcessorThread;

	private Map<String, RaftPartition> partitionMap;

	RaftNode(String[] nodes, int id, String logFilePrefix) throws RaftNodeException {
		this(nodes, id, logFilePrefix, null);
	}

	RaftNode(String[] nodes, int id, String logFilePrefix, Messager messager) throws RaftNodeException {
		this.id = id;
		this.logFilePrefix = logFilePrefix;
		if (messager == null) {
			try {
				messager = MessagerFactory.newMessager("tcp", 12000 + id, new RaftMessageHandler(), RECEIVE_TIMEOUT, new TimeoutHandler());
			} catch (RaftRpcException e) {
				throw new RaftNodeException(e);
			}
		}
		converter = messager.getObjectDataConverter();
		senders = new Sender[nodes.length];
		try {
			for (int i = 0; i < senders.length; i++) {
				senders[i] = messager.getSender(nodes[i], 12000 + i);
			}
			receiver = messager.getReceiver();
		} catch (RaftRpcException e) {
			throw new RuntimeException(e);
		}
		partitionMap = new ConcurrentHashMap<>();
	}

	public LogProposal getLogProposal(StateMachine fsm) throws RaftNodeException {
		RaftPartition partition = partitionMap.get(fsm.getName());
		if (partition != null) {
			return partition;
		}
		partitionMap.put(fsm.getName(), partition = new RaftPartition(id, senders, logFilePrefix, fsm, converter, INACTIVITY_TIMEOUT));
		return partition;
	}

	public void start() throws RaftNodeException {
		messageProcessorThread = new Thread(new MessageProcessor());
		messageProcessorThread.setName(String.format("RaftNode %d", id));
		messageProcessorThread.setDaemon(true);
		messageProcessorThread.start();
	}

	public void stop() {
		receiver.terminate();
		for (Sender s : senders) {
			s.terminate();
		}
		messageProcessorThread.interrupt();
		try {
			messageProcessorThread.join(10000);
		} catch (InterruptedException e) {
		}
	}

	private class MessageProcessor implements Runnable {
		@Override
		public void run() {
			while (!Thread.interrupted()) {
				receiver.handleRequest();
			}
		}
	}

	private class TimeoutHandler implements MessageHandler {
		@Override
		public void handle(Object object) throws RaftRpcException {
			for (RaftPartition p : partitionMap.values()) {
				p.handleTimeout();
			}
		}
	}

	private class RaftMessageHandler implements MessageHandler {
		@Override
		public void handle(Object object) throws RaftRpcException {
			if (object instanceof RaftMessage) {
				RaftMessage raftMsg = (RaftMessage)object;
				logger.trace("receive {} {}", raftMsg.getFsmId(), new String(converter.toBytes(object)));
				RaftPartition p = partitionMap.get(raftMsg.getFsmId());
				p.handleRaftMessage(object);
			}
		}
	}
}
