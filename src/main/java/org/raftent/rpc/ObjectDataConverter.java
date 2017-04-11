package org.raftent.rpc;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface ObjectDataConverter {
	static final ObjectMapper mapper = new ObjectMapper();
	default byte[] toBytes(Object data) throws RaftRpcException {
		StringBuilder s = new StringBuilder();
		try {
			s.append(data.getClass().getName()).append('\t').append(mapper.writeValueAsString(data));
			return s.toString().getBytes();
		} catch (JsonProcessingException e) {
			throw new RaftRpcException(e);
		}
	}

	default Object toObject(byte[] data) throws RaftRpcException {
		String d = new String(data);
		int sep = d.indexOf('\t');
		try {
			String clazzName = d.substring(0, sep);
			String json = d.substring(sep + 1);
			Class<?> clazz = Class.forName(clazzName);
			return mapper.readValue(json, clazz);
		} catch (ClassNotFoundException | IOException e) {
			throw new RaftRpcException(e);
		}
	}
}
