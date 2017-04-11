package org.raftent.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.raftent.node.RaftNodeException;

class PersistentStore {
	private static final int MAX_LOG_BUFFER = 1024*1024*1024;
	private static final int POS_CURRENT_TERM   = 0;
	private static final int POS_VOTED_FOR      = 8;
	private static final int POS_LAST_LOG_INDEX = 16;
	private static final int POS_FIRST_LOG_INDEX = 24;
	private static final int POS_INDEX_0         = 32;
	private static final int POS_DATA_INDEX      = 0;

	private MappedByteBuffer indexBuffer;
	private MappedByteBuffer dataBuffer;

	PersistentStore(String logFilePrefix) throws RaftNodeException {
		boolean newLog = false;
		try (RandomAccessFile indexFileHandle = new RandomAccessFile(String.format("%s.idx", logFilePrefix), "rw");
				FileChannel indexFileChannel = indexFileHandle.getChannel();
				RandomAccessFile dataFileHandle = new RandomAccessFile(String.format("%s.dat", logFilePrefix), "rw");
				FileChannel dataFileChannel = dataFileHandle.getChannel();) {
			if (indexFileChannel.size() == 0) {
				newLog = true;
			}
			indexBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_LOG_BUFFER);
			if (newLog) {
				setCurrentTerm(0);
				setVotedFor(-1);
				setLastLogIndex(0);
				setFirstLogIndex(0);
			}
			dataBuffer = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_LOG_BUFFER);
			if (newLog) {
				dataBuffer.position(POS_DATA_INDEX);
				dataBuffer.putLong(8);
			}
		} catch (IOException e) {
			throw new RaftNodeException(e);
		}
	}

	void setCurrentTerm(long currentTerm) {
		indexBuffer.position(POS_CURRENT_TERM);
		indexBuffer.putLong(currentTerm);
		setVotedFor(-1);
	}

	long getCurrentTerm() {
		indexBuffer.position(POS_CURRENT_TERM);
		return indexBuffer.getLong();
	}

	void setVotedFor(long votedFor) {
		indexBuffer.position(POS_VOTED_FOR);
		indexBuffer.putLong(votedFor);
	}

	int getVotedFor() {
		indexBuffer.position(POS_VOTED_FOR);
		return (int)indexBuffer.getLong();		
	}

	void setLastLogIndex(long lastLogIndex) {
		indexBuffer.position(POS_LAST_LOG_INDEX);
		indexBuffer.putLong(lastLogIndex);
	}

	long getLastLogIndex() {
		indexBuffer.position(POS_LAST_LOG_INDEX);
		return indexBuffer.getLong();		
	}

	void setFirstLogIndex(long firstLogIndex) {
		indexBuffer.position(POS_FIRST_LOG_INDEX);
		indexBuffer.putLong(firstLogIndex);
	}

	void setLogEntry(long index, byte[] data) {
		long pos;
		dataBuffer.position(POS_DATA_INDEX);
		pos = dataBuffer.getLong();
		long dataPos = pos;
		dataBuffer.position((int)pos);
		dataBuffer.putLong(data.length);
		dataBuffer.put(data);
		pos = dataBuffer.position();
		dataBuffer.position(POS_DATA_INDEX);
		dataBuffer.putLong(pos);
		pos = POS_INDEX_0 + (index * 8);
		indexBuffer.position((int)pos);
		indexBuffer.putLong(dataPos);
		dataBuffer.force();
		indexBuffer.force();
	}

	byte[] getLogEntry(long index) {
		long pos;
		pos = POS_INDEX_0 + index * 8;
		indexBuffer.position((int)pos);
		long dataPos = indexBuffer.getLong();
		dataBuffer.position((int)dataPos);
		long dataLen = dataBuffer.getLong();
		byte[] data = new byte[(int)dataLen];
		dataBuffer.get(data);
		return data;
	}
}
