package org.raftent.rpc;

import org.raftent.rpc.datagram.UdpMessager;
import org.raftent.rpc.stream.TcpMessager;

public class MessagerFactory {
	public static Messager newMessager(String type, int receiverPort, MessageHandler handler, int timeout, MessageHandler timeoutHandler) throws RaftRpcException {
		switch (type) {
		case "tcp":
			return new TcpMessager(receiverPort, handler, timeout, timeoutHandler);
		case "udp":
			return new UdpMessager(receiverPort, handler, timeout, timeoutHandler);
		default:
			return null;
		}
	}
}
