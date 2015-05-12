
package HadoopXmlExtractor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class XmlReducerPartitioner extends
	Partitioner<XmlReducerKeyWritable, Text> {

	@Override
	public int getPartition(XmlReducerKeyWritable key, Text value, int numReduceTasks) {
		int partition = 0; 
		if (key != null){
			if (!key.getFilename().isEmpty()){
				partition = (key.getFilename().hashCode() % numReduceTasks);
			}
		}
		return partition;
	}
}