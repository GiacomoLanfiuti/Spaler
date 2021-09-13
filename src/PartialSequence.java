package spaler;

/**
 * This class is used to store fragments of a long input FASTA 
 * sequence as an array of bytes.
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0  
 */
public class PartialSequence extends Record { 
	 
	private static final long serialVersionUID = 6820343484812253133L; 

	private String header;
	private int bytesToProcess;

	@Override
	public String getKey(){
		return header;
	}
	
	public String getValue2(){
		return new String(getBuffer(), getStartValue(), bytesToProcess);
	}
	
	public String toString2() {
		return getStartValue() > 0 ? ">" + getKey() + "\n" + getValue2() : getValue2();
	}

	@Override
	public String toString() {
		return "PartialSequence [header=" + header + 
				", bufferSize=" + getValueLength() +
				", startValue=" + getStartValue() + 
				", endValue=" + getEndValue() + 
				", bytesToProcess=" + bytesToProcess + "]";
	}

	public int getBytesToProcess() {
		return bytesToProcess;
	}

	public void setBytesToProcess(int bytesToProcess) {
		this.bytesToProcess = bytesToProcess;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}
}