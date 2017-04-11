package org.raftent.rpc.datagram;

import org.raftent.rpc.MessageHandler;
import org.raftent.rpc.Messager;
import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Receiver;
import org.raftent.rpc.Sender;

public class UdpMessager implements Messager, ObjectDataConverter {
	private int receiverPort;
	private MessageHandler handler;
	private int timeout;
	private MessageHandler timeoutHandler;
	public UdpMessager(int receiverPort, MessageHandler handler, int timeout, MessageHandler timeoutHandler) {
		this.receiverPort = receiverPort;
		this.handler = handler;
		this.timeout = timeout;
		this.timeoutHandler = timeoutHandler;
	}

	@Override
	public Sender getSender(String destination, int port) throws RaftRpcException {
		return new UdpSenderImpl(destination, port, this);
	}

	@Override
	public Receiver getReceiver() throws RaftRpcException {
		return new UdpReceiverImpl(receiverPort, this, handler, timeout, timeoutHandler);
	}

	@Override
	public ObjectDataConverter getObjectDataConverter() {
		return this;
	}
}
