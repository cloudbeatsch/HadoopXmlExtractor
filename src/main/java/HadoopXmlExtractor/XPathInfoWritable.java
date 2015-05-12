package HadoopXmlExtractor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class XPathInfoWritable implements Writable {
	public XPathInfoWritable(){
		this.pos = 0;
		this.expression = "";
	}

	public XPathInfoWritable(int pos, String expression){
		this.pos = pos;
		this.expression = expression;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(pos).append("\t")
			.append(expression)).toString();
	}

 	@Override
	public void readFields(DataInput dataInput) throws IOException {
		pos = WritableUtils.readVInt(dataInput);
		expression = WritableUtils.readString(dataInput);
	}

 	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVInt(dataOutput, pos);
		WritableUtils.writeString(dataOutput, expression);
	}

	private int pos;
	public int getPos(){
		return pos;
	}

	private String expression;
	public String getExpression(){
		return expression;
	}
}