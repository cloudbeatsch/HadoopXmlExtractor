
package HadoopXmlExtractor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class XmlReducerGroupingComparator extends WritableComparator {
  protected XmlReducerGroupingComparator() {
		super(XmlReducerKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		XmlReducerKeyWritable key1 = (XmlReducerKeyWritable) w1;
		XmlReducerKeyWritable key2 = (XmlReducerKeyWritable) w2;

		return key1.getFilename().compareTo(key2.getFilename());
	}
}