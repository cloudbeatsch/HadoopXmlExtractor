
package HadoopXmlExtractor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class XmlReducerKeyWritable implements Writable,
	WritableComparable<XmlReducerKeyWritable> {
	public XmlReducerKeyWritable() {
	}
	
	private String filename;
	private int order;
	private int sequence;

	public XmlReducerKeyWritable(String filename, int sequence, int order) {
		this.filename = filename;
		this.sequence = sequence;
		this.order = order;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(filename).append("\t")
			.append(sequence).append("\t")
			.append(order)).toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		filename = WritableUtils.readString(dataInput);
		sequence = WritableUtils.readVInt(dataInput);
		order = WritableUtils.readVInt(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, filename);
		WritableUtils.writeVInt(dataOutput, sequence); 
		WritableUtils.writeVInt(dataOutput, order); 
	}

	// this can be optimized
	public int compareTo(XmlReducerKeyWritable objKeyPair) {
		int result = filename.compareTo(objKeyPair.filename);
		if (0 == result) {
			result = order - objKeyPair.order;
		}
		return result;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}
}