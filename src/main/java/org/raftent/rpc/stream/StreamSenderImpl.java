package org.raftent.rpc.stream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.raftent.impl.RaftMessage;
import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamSenderImpl implements Sender {
	private static final Logger logger = LoggerFactory.getLogger(StreamSenderImpl.class);
	private SocketChannel socketChannel;
	private SocketAddress addr;
	private volatile boolean terminated;
	private ObjectDataConverter converter;
	private Selector selector;
	private SelectionKey socketKey;
	private long lastConnectTry;

	StreamSenderImpl(String host, int port, ObjectDataConverter converter, Selector selector) throws RaftRpcException {
		this.converter = converter;
		this.selector = selector;
		terminated = false;
		addr = new InetSocketAddress(host, port);
		reconnect();
	}

	@Override
	public void send(Object data) throws RaftRpcException {
		if (terminated) {
			return;
		}
		if (socketChannel.isConnected()) {
			synchronized (socketKey) {
				if (socketKey.attachment() == null) {
					byte[] dataBytes = converter.toBytes(data);
					ByteBuffer dataBuffer  = ByteBuffer.allocate(4 + dataBytes.length);
					dataBuffer.putInt(dataBytes.length);
					dataBuffer.put(dataBytes);
					dataBuffer.flip();
					socketKey.attach(dataBuffer);
					RaftMessage raftMsg = (RaftMessage)data;
					logger.trace("send    {} {}", raftMsg.getFsmId(), new String(dataBytes));
				}
			}
		}
	}

	@Override
	public void terminate() {
		try {
			terminated = true;
			socketChannel.close();
		} catch (IOException e) {
			logger.trace("terminate exception", e);
		}
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}
	
	public long getLastConnectTry() {
		return lastConnectTry;
	}

	void reconnect() throws RaftRpcException {
		try {
			socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);
			socketChannel.connect(addr);
			socketKey = socketChannel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE);
			lastConnectTry = System.currentTimeMillis();
		} catch (IOException e) {
			throw new RaftRpcException(e);
		}
	}
}
