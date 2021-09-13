package spaler;

import java.io.Serializable;

/**
 * Utility class used to represent as a record a sequence existing 
 * in a FASTA file.
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0
 */

public class Record implements Serializable { 
	 
	private static final long serialVersionUID = 8015377043208271607L; 
	 
	private byte[] buffer;
	private int startKey, endKey;
	private int startValue, endValue;
	private long startSplit, splitOffset;
	private String fileName;

	public String getKey() {
		return new String(buffer, startKey, getKeyLength());
	}

	public String getValue() {
		return new String(buffer, startValue, getValueLength());
	}

	@Override
	public String toString() {
		return ">" + this.getKey() + "\n" + this.getValue();
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}

	public int getStartKey() {
		return startKey;
	}

	public void setStartKey(int startKey) {
		this.startKey = startKey;
	}

	public int getEndKey() {
		return endKey;
	}

	public void setEndKey(int endKey) {
		this.endKey = endKey;
	}

	public int getStartValue() {
		return startValue;
	}

	public void setStartValue(int startValue) {
		this.startValue = startValue;
	}

	public int getEndValue() {
		return endValue;
	}

	public void setEndValue(int endValue) {
		this.endValue = endValue;
	}

	public int getKeyLength() {
		return endKey - startKey + 1;
	}
	
	public int getValueLength() {
		return endValue - startValue + 1;
	}

	public void setStartSplit(long startSplit) {
		this.startSplit = startSplit;
	}

	public long getStartSplit() {
		return startSplit;
	}

	public void setSplitOffset(long offset) {
		this.splitOffset = offset;
	}

	public long getSplitOffset() {
		return splitOffset;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public long getFileOffset() {
		return startSplit + splitOffset;
	}
}