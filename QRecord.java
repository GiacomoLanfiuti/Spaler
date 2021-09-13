package spaler;
/**
 * Utility class used to represent as a record a sequence existing 
 * in a FASTQ file.
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0
 */
public class QRecord extends Record { 
	 
	private static final long serialVersionUID = -7555239567456193078L; 

	private int startKey2, endKey2;
	private int startQuality, endQuality;

	public String getKey2() { 
		return new String(getBuffer(), startKey2, getKey2Length()); 
	}

	public String getQuality() { 
		return new String(getBuffer(), startQuality, getQualityLength()); 
	}

	@Override
	public String toString() {
		return "@" + getKey() + "\n" + getValue() + "\n+" + getKey2() + "\n" + getQuality();
	}

	public int getStartKey2() {
		return startKey2;
	}

	public void setStartKey2(int startKey2) {
		this.startKey2 = startKey2;
	}

	public int getEndKey2() {
		return endKey2;
	}

	public void setEndKey2(int endKey2) {
		this.endKey2 = endKey2;
	}

	public int getStartQuality() {
		return startQuality;
	}

	public void setStartQuality(int startQuality) {
		this.startQuality = startQuality;
	}

	public int getEndQuality() {
		return endQuality;
	}

	public void setEndQuality(int endQuality) {
		this.endQuality = endQuality;
	}

	public int getKey2Length() {
		return endKey2 - startKey2 + 1;
	}
	
	public int getQualityLength() {
		return endQuality - startQuality + 1;
	}
}
