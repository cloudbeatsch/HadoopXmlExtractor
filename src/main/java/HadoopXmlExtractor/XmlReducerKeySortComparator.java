
package HadoopXmlExtractor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class XmlReducerKeySortComparator extends WritableComparator {
	protected XmlReducerKeySortComparator() {
		super(XmlReducerKeyWritable.class, true);
	}
	 
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		XmlReducerKeyWritable key1 = (XmlReducerKeyWritable) w1;
		XmlReducerKeyWritable key2 = (XmlReducerKeyWritable) w2;

		int cmpResult = key1.getFilename().compareTo(key2.getFilename());
		if (cmpResult == 0)
		{
			cmpResult = key1.getSequence()- key2.getSequence();
			if (cmpResult == 0)
			{
				return key1.getOrder()- key2.getOrder();
			}
		}
		return cmpResult;
	}
}