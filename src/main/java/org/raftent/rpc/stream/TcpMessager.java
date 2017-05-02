package org.raftent.rpc.stream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.raftent.rpc.MessageHandler;
import org.raftent.rpc.Messager;
import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Receiver;
import org.raftent.rpc.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpMessager implements Messager, ObjectDataConverter, Receiver {
	private static final Logger logger = LoggerFactory.getLogger(TcpMessager.class);
	private MessageHandler handler;
	private int timeout;
	private MessageHandler timeoutHandler;
	private Selector selector;
	private ServerSocketChannel serverChannel;
	private Map<SocketChannel, StreamSenderImpl> senderMap;
	private volatile boolean terminated;

	public TcpMessager(int receiverPort, MessageHandler handler, int timeout, MessageHandler timeoutHandler) throws RaftRpcException {
		this.handler = handler;
		this.timeout = timeout;
		this.timeoutHandler = timeoutHandler;
		terminated = false;
		senderMap = new ConcurrentHashMap<>();
		try {
			selector = Selector.open();
			serverChannel = ServerSocketChannel.open();
			serverChannel.configureBlocking(false);
			serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			InetSocketAddress bindAddr;
			serverChannel.bind(bindAddr = new InetSocketAddress(receiverPort));
			logger.debug("listen on {}", bindAddr);
			serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			throw new RaftRpcException(e);
		}
	}

	@Override
	public Sender getSender(String destination, int port) throws RaftRpcException {
		StreamSenderImpl sender =  new StreamSenderImpl(destination, port, this, selector);
		senderMap.put(sender.getSocketChannel(), sender);
		return sender;
	}

	@Override
	public Receiver getReceiver() throws RaftRpcException {
		return this;
	}

	@Override
	public void handleRequest() {
		if (terminated) {
			return;
		}
		try {
			selector.select(timeout);
		} catch (IOException e) {
			return;
		}
		Set<SelectionKey> keys = null;
		try {
			keys = selector.selectedKeys();
		} catch (Exception e) {
			logger.debug("Failed to get selected keys", e);
		}
		try {
			timeoutHandler.handle(null);
		} catch (RaftRpcException e) {
			logger.debug("Failed to handle timeout", e);
		}
		if (keys == null) {
			return;
		}
		for (SelectionKey k : keys) {
			if (k.isValid() && k.isAcceptable()) {
				SocketChannel channel;
				try {
					channel = serverChannel.accept();
					channel.configureBlocking(false);
					channel.register(selector, SelectionKey.OP_READ);
					logger.debug("accept from {}", channel);
				} catch (IOException e) {
					logger.debug("Failed to accept a connection", e);
				}
			}
			if (k.isValid() && k.isReadable()) {
				SocketChannel channel = (SocketChannel)k.channel();
				ByteBuffer dataBytes;
				ByteBuffer mdata = (ByteBuffer)k.attachment();
				if (mdata != null) {
					dataBytes = mdata;
				}
				else {
					ByteBuffer len = ByteBuffer.allocate(4);
					try {
						if (-1 == channel.read(len)) {
							channel.close();
							continue;
						}
					} catch (IOException e) {
						logger.debug("Failed to read len", e);
						continue;
					}
					len.flip();
					dataBytes = ByteBuffer.allocate(len.getInt());
				}
				try {
					if (-1 == channel.read(dataBytes)) {
						channel.close();
						continue;
					}
				} catch (IOException e) {
					logger.debug("Failed to read data", e);
					continue;
				}
				if (dataBytes.remaining() != 0) {
					if (mdata == null) {
						k.attach(dataBytes);
					}
					continue;
				}
				Object object;
				try {
					object = toObject(dataBytes.array());
					if (mdata != null) {
						k.attach(null);
					}
					handler.handle(object);
				} catch (RaftRpcException e) {
					logger.debug("Failed to process data", e);
					continue;
				}
			}
			if (k.isValid() && k.isConnectable()) {
				SocketChannel channel = (SocketChannel)k.channel();
				try {
					StreamSenderImpl sender = senderMap.get(channel);
					if (sender == null || (System.currentTimeMillis() > (sender.getLastConnectTry() + 5000))) {
						channel.finishConnect();
						channel.configureBlocking(false);
						logger.debug("finish connect to {}", channel);
					}
				} catch (IOException e) {
					logger.debug("Failed to complete connection", e);
					updateWriteChannle(k);
				}
			}
			if (k.isValid() && k.isWritable()) {
				synchronized (k) {
					SocketChannel channel = (SocketChannel)k.channel();
					ByteBuffer dataBuffer = (ByteBuffer)k.attachment();
					if (dataBuffer != null) {
						if (dataBuffer.remaining() != 0) {
							try {
								channel.write(dataBuffer);
							} catch (IOException e) {
								logger.debug("Failed to write data", e);
								updateWriteChannle(k);
							}
						}
						if (dataBuffer.remaining() == 0) {
							k.attach(null);
						}
					}
				}
			}
		}
		keys.clear();
	}

	@Override
	public void terminate() {
		try {
			terminated = true;
			selector.close();
		} catch (IOException e) {
			logger.debug("Failed to terminated selector", e);
		}
	}

	@Override
	public ObjectDataConverter getObjectDataConverter() {
		return this;
	}
	
	private void updateWriteChannle(SelectionKey k) {
		SocketChannel channel = (SocketChannel)k.channel();
		StreamSenderImpl sender = senderMap.get(channel);
		if (sender != null) {
			senderMap.remove(channel);
			try {
				sender.reconnect();
				k.attach(null);
				k.cancel();
			} catch (RaftRpcException e) {
				logger.debug("Failed to cancel key", e);
			}
			senderMap.put(sender.getSocketChannel(), sender);
		}
	}
}
