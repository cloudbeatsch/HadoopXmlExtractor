package HadoopXmlExtractor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;

public class XmlMapperValueWritable implements Writable {
	private Text xmlStream;
	private int sequence;
	private XPathInfoWritable[] xPathExpressions;

	public XmlMapperValueWritable()
	{
		this.xmlStream = null;
		this.sequence = sequence;
		this.xPathExpressions = null;
	}

	public XmlMapperValueWritable(Text xmlStream, int sequence, XPathInfoWritable[] xPathExpressions) {
		this.xmlStream = xmlStream;
		this.sequence = sequence;
		this.xPathExpressions = xPathExpressions;
	}

 	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(xmlStream.toString()).append("\t");
		sb.append(sequence).append("\t");
		for (int i=0; i<xPathExpressions.length; i++) {
			sb.append(xPathExpressions[i]).append("\t");
		}
		return sb.toString();
	}

 	@Override
	public void readFields(DataInput dataInput) throws IOException {
		xmlStream.set(WritableUtils.readCompressedByteArray(dataInput));
		sequence = WritableUtils.readVInt(dataInput);
		int len = WritableUtils.readVInt(dataInput);
		xPathExpressions = new XPathInfoWritable[len];
		for (int i=0; i<len; i++) {
			xPathExpressions[i] = new XPathInfoWritable();
			xPathExpressions[i].readFields(dataInput);
		}
	}

 	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeCompressedByteArray(dataOutput, xmlStream.getBytes());
		WritableUtils.writeVInt(dataOutput, sequence); 
		WritableUtils.writeVInt(dataOutput, xPathExpressions.length); 
		for (int i=0; i<xPathExpressions.length; i++) {
			 xPathExpressions[i].write(dataOutput);
		}
	}
	
	public Text getXmlStream() {
		return xmlStream;
	}

	public void setXmlStream(Text xmlStream) {
		this.xmlStream = xmlStream;
	}

	public XPathInfoWritable[] getXPathExpressions() {
		return xPathExpressions;
	}

	public void setXPathExpressions(XPathInfoWritable[] xPathExpressions) {
		this.xPathExpressions = xPathExpressions;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
 }