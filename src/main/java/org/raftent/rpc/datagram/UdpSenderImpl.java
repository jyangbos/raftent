package org.raftent.rpc.datagram;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;

import org.raftent.rpc.ObjectDataConverter;
import org.raftent.rpc.RaftRpcException;
import org.raftent.rpc.Sender;

class UdpSenderImpl  implements Sender {
	private SocketAddress address;
	private DatagramSocket socket;
	private ObjectDataConverter converter;

	public UdpSenderImpl(String host, int port, ObjectDataConverter converter) throws RaftRpcException {
		try {
			socket = new DatagramSocket();
			address = new InetSocketAddress(host, port);
			this.converter = converter;
		} catch (SocketException e) {
			throw new RaftRpcException(e);
		}
	}

	@Override
	public void send(Object data) throws RaftRpcException {
		byte[] dataBytes = converter.toBytes(data);
		DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length, address);
		try {
			socket.send(packet);
		} catch (IOException e) {
			throw new RaftRpcException(e);
		}
	}

	@Override
	public void terminate() {
		socket.close();
	}
}
