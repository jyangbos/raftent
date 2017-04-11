package org.raftent.rpc.datagram;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.raftent.rpc.MessageHandler;
import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UdpReceiverImpl implements Receiver {
	private static final Logger logger = LoggerFactory.getLogger(UdpReceiverImpl.class);
	private static final int MAX_PACKET_SIZE = 1024;

	private DatagramSocket socket;
	private ObjectDataConverter converter;
	private MessageHandler handler, timeoutHandler;
	private DatagramPacket packet;

	public UdpReceiverImpl(int port, ObjectDataConverter converter,
			MessageHandler handler, int timeout, MessageHandler timeoutHandler) throws RaftRpcException {
		try {
			this.converter = converter;
			this.handler = handler;
			this.timeoutHandler = timeoutHandler;
			socket = new DatagramSocket(port);
			socket.setSoTimeout(timeout);
			packet = new DatagramPacket(new byte[MAX_PACKET_SIZE], MAX_PACKET_SIZE);
		} catch (SocketException e) {
			throw new RaftRpcException(e);
		}
	}

	@Override
	public void handleRequest() {
		try {
			socket.receive(packet);
			Object object = converter.toObject(packet.getData());
			handler.handle(object);
		} catch (SocketTimeoutException e) {
			try {
				timeoutHandler.handle(null);
			} catch (RaftRpcException e1) {
			}
		} catch (SocketException e) {
			if (!"Socket closed".equals(e.getMessage())) {
				logger.debug("Encountered socket exception", e);
			}
		} catch (IOException | RaftRpcException e) {
			if (e.getCause() instanceof SocketException) {
				if ("Socket is closed".equals(e.getCause().getMessage())) {
					return;
				}
			}
			logger.debug("Encountered io exception", e);
		}
	}

	@Override
	public void terminate() {
		socket.close();
	}
}
