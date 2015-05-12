
package HadoopXmlExtractor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class XmlReducerValueWritable implements Writable {
	public XmlReducerValueWritable() {
	}
	
	private String value;
	private int order;

	public XmlReducerValueWritable(String value, int order) {
		this.value = value;
		this.order = order;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(value).append("\t");
		sb.append(order);
		return sb.toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		value = WritableUtils.readString(dataInput);
		order = WritableUtils.readVInt(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, value);
		WritableUtils.writeVInt(dataOutput, order); 
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}
}