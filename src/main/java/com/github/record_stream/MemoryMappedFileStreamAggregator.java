package com.github.record_stream;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.DoubleBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.camel.util.FileUtil;

public class MemoryMappedFileStreamAggregator implements StreamAggregator<String>, Closeable {
	private int /* minKey, maxKey, */adjustKey, countOffset;
	private String fileName;
	private FileChannel channel;
	private MappedByteBuffer buffer;
	private DoubleBuffer intKeysBuffer;
	/**
	 * Entry Key: count position; Value: sum position
	 */
	private Map<String, Entry<Integer, Integer>> offsets;

	public MemoryMappedFileStreamAggregator(int minimumKey, int maximumKey, String fileName, String... keys) throws IOException {
		// minKey = minimumKey; // 1
		// maxKey = maximumKey; // 31
		adjustKey = minimumKey * -1; // -1
		countOffset = maximumKey + adjustKey + 1; // 32
		this.fileName = fileName;
		File state = new File(fileName);
		int totalKeyLengthsAndValues = 0;
		if(keys!=null) {
			offsets = new HashMap<>(keys.length, 1);
			for (String key : keys) {
				totalKeyLengthsAndValues += key.getBytes(StandardCharsets.UTF_8).length + Integer.BYTES + Double.BYTES * 2;
				offsets.put(key, null);
			}
		}
		long size = (maximumKey + adjustKey + 1) * 2 * Double.BYTES + totalKeyLengthsAndValues;
		if (!state.canRead()) {
			assert state.createNewFile();
		}
		@SuppressWarnings("resource")
		// FileChannel is closed in close()
		RandomAccessFile raf = new RandomAccessFile(state, "rw");
		channel = raf.getChannel();
		boolean verifyKeys=false;
		if(raf.length()==size) {
			verifyKeys=true;
		} else {
			if (raf.length() > 0) {
				FileUtil.copyFile(state, new File(fileName + ".backup"));
			}
			raf.setLength(size);
		}
		buffer = channel.map(MapMode.READ_WRITE, 0, size);
		intKeysBuffer = buffer.asDoubleBuffer();
		if(totalKeyLengthsAndValues>0) {
			int stringKeysStart = (maximumKey + adjustKey + 1) * 2 * Double.BYTES;
			buffer.limit(buffer.capacity());
			buffer.position(stringKeysStart);
			if(verifyKeys) {
				byte[] keyBytes = new byte[1024];
				while (buffer.position() < buffer.limit()) {
					int keySize = buffer.getInt();
					buffer.get(keyBytes, 0, keySize);
					String key = new String(keyBytes, StandardCharsets.UTF_8);
					Entry<Integer, Integer> position = new SimpleEntry<>(buffer.position(), null);
					buffer.getDouble();
					position.setValue(buffer.position());
					buffer.getDouble();
					offsets.put(key, position);
				}
			} else {
				for (String key : keys) {
					byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
					buffer.putInt(keyBytes.length);
					buffer.put(keyBytes);
					Entry<Integer, Integer> position = new SimpleEntry<>(buffer.position(), null);
					buffer.putDouble(0);
					position.setValue(buffer.position());
					buffer.putDouble(0);
					offsets.put(key, position);
				}
			}
		}
		buffer.limit(buffer.capacity());
	}

	@Override
	public long incr(int key) {
		return count(key, 1);
	}

	@Override
	public long incr(String key) {
		return count(key, 1);
	}

	@Override
	public long count(int key, long value) {
		return (long) sumOffset(getCountOffset(key), value);
	}

	@Override
	public long count(String key, long value) {
		return (long) sumBufferOffset(offsets.get(key).getKey(), value);
	}

	@Override
	public double sum(int key, double value) {
		return sumOffset(getSumOffset(key), value);
	}

	@Override
	public double sum(String key, double value) {
		return sumBufferOffset(offsets.get(key).getValue(), value);
	}

	private double sumOffset(int offset, double value) {
		double answer = 0;
		synchronized (buffer) {
			answer = intKeysBuffer.get(offset) + value;
			intKeysBuffer.put(offset, answer);
		}
		return answer;
	}

	private double sumBufferOffset(int offset, double value) {
		double answer = 0;
		synchronized (buffer) {
			buffer.position(offset);
			answer = buffer.getDouble() + value;
			buffer.position(offset);
			buffer.putDouble(answer);
		}
		return answer;
	}

	@Override
	public double average(int key, double value) {
		return sum(key, value) / (double) count(key, 1);
	}

	@Override
	public double average(String key, double value) {
		return sum(key, value) / count(key, 1);
	}

	@Override
	public long getCount(int key) {
		return (long) intKeysBuffer.get(getCountOffset(key));
	}

	@Override
	public long getCount(String key) {
		int offset = offsets.get(key).getKey();
		synchronized (buffer) {
			buffer.position(offset);
			return (long) buffer.getDouble();
		}
	}

	@Override
	public double getSum(int key) {
		return intKeysBuffer.get(getSumOffset(key));
	}

	@Override
	public double getSum(String key) {
		int offset = offsets.get(key).getValue();
		synchronized (buffer) {
			buffer.position(offset);
			return buffer.getDouble();
		}
	}

	@Override
	public void setCount(int key, double value) {
		int offset = getCountOffset(key);
		synchronized (buffer) {
			intKeysBuffer.put(offset, value);
		}
	}

	@Override
	public void setCount(String key, double value) {
		int offset = offsets.get(key).getKey();
		synchronized (buffer) {
			buffer.position(offset);
			buffer.putDouble(value);
		}
	}

	@Override
	public void setSum(int key, double value) {
		int offset = getSumOffset(key);
		synchronized (buffer) {
			intKeysBuffer.put(offset, value);
		}
	}

	@Override
	public void setSum(String key, double value) {
		int offset = offsets.get(key).getValue();
		synchronized (buffer) {
			buffer.position(offset);
			buffer.putDouble(value);
		}
	}

	public void delete() throws IOException {
		close();
		new File(fileName).delete();
	}

	@Override
	public void close() throws IOException {
		if (channel != null && channel.isOpen()) {
			channel.close();
		}
		intKeysBuffer = null;
		buffer = null;
	}

	private int getCountOffset(int key) {
		return key + adjustKey + countOffset;
	}

	private int getSumOffset(int key) {
		return key + adjustKey;
	}
}
