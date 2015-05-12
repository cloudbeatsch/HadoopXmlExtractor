package HadoopXmlExtractor;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamConstants;

import java.io.*;
import java.util.Stack;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.*;
import static org.w3c.dom.Node.*;
import org.xml.sax.SAXException;

import javax.xml.stream.*;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class XmlExtractor {

	public static final String DELIMITOR_STRING_KEY = "xmlextractor.delimiter_string";
	public static final String PARSING_NODES_KEY = "xmlextractor.nodes";
	public static final String SORT_ORDER_DELIMITOR_KEY = "xmlextractor.sort_order_delimiter_string";
	public static final String NR_OF_COLUMNS_KEY = "xmlextractor.nr_of_columns";
	public static final String OUTPUT_DELIMITOR_KEY = "xmlextractor.output_delimiter_string";
	
	public static class SeqXmlInputFormat extends FileInputFormat<Text, XmlMapperValueWritable> {
		
		public RecordReader<Text, XmlMapperValueWritable> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new SeqXmlRecordReader();
		}
		
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}
	}

	public static class Map extends Mapper<Text, XmlMapperValueWritable, XmlReducerKeyWritable, XmlReducerValueWritable> {

		String delim;
		String sortOrderDelim;

		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		}
		
		@Override
		protected void map(Text key, XmlMapperValueWritable value, Mapper.Context context)
				throws IOException, InterruptedException {
			try {

				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				factory.setNamespaceAware(true);
				DocumentBuilder builder = factory.newDocumentBuilder();
			
				// converting to a string to remove trailing non-white space characters because they cause
				// a "content is not allowed in trailing section" error
				String docString = value.getXmlStream().toString();
				Document doc = builder.parse(new ByteArrayInputStream(docString.getBytes("utf-8")));
				XPathFactory xpathfactory = XPathFactory.newInstance();
				XPath xpath = xpathfactory.newXPath();
				
				// now we iterate across all the XPath expressions for this xmlStream
   				// and run them against the DOM	
				for (XPathInfoWritable info : value.getXPathExpressions()){
					try {

						XPathExpression expr = xpath.compile(info.getExpression());
						Object result = expr.evaluate(doc, XPathConstants.NODESET);
						NodeList nodes = (NodeList) result;
						
						// in the case we got matching nodes, we create the reducer input
						if (nodes.getLength() > 0) {
							StringBuilder sb = new StringBuilder();
							for (int i = 0; i < nodes.getLength(); i++) {
								sb.append(nodes.item(i).getNodeValue());
							}

							XmlReducerKeyWritable xmlKey = new XmlReducerKeyWritable(key.toString(), value.getSequence(), info.getPos());
							context.write(xmlKey, new XmlReducerValueWritable(sb.toString(), info.getPos()));
						}
					}
					catch (Exception ex) {
						System.out.println(ex.toString());
					}		
				}

			}
			catch (ParserConfigurationException ex) {
				System.out.println(ex.toString());
			}
			catch (SAXException ex) {
				System.out.println(ex.toString());
			}
		}
	}

	public static class Reduce extends Reducer<XmlReducerKeyWritable, XmlReducerValueWritable, NullWritable, Text> {

		private int nrOfColumns=0;
		private String delim;
		private String outputDelim;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			nrOfColumns = conf.getInt(NR_OF_COLUMNS_KEY, 0);
			delim = conf.get(DELIMITOR_STRING_KEY);
			outputDelim = conf.get(OUTPUT_DELIMITOR_KEY);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}

		public void reduce(XmlReducerKeyWritable key, Iterable<XmlReducerValueWritable> values, Context context)
				throws IOException, InterruptedException {
			String[] theColumns = new String[nrOfColumns];
			
			for (XmlReducerValueWritable value : values) {
				theColumns[value.getOrder()] = value.getValue();

				// once we got all columns, we write them - this works because 
				// values are sorted by sequence and then columns
				if (value.getOrder() == (nrOfColumns -1)) {
					StringBuilder sb = new StringBuilder();
					for (String col: theColumns) {
						if ((col != null) && (!col.isEmpty())) {
							sb.append(col);
						}
						else {
							sb.append(' ');
						}
						// writing the column delimiter e.g. \t
						sb.append(outputDelim);					
					}
					// writing the reducer output
					context.write(null ,new Text (sb.toString()));	
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: XmlExtractor <in path> <out path> <config> ");
			System.exit(2);
		}
		conf.addResource(new Path(otherArgs[2]));

		Job job = new Job(conf, "XmlExtractor");
		job.setJarByClass(XmlExtractor.class);
		job.setMapOutputKeyClass(XmlReducerKeyWritable.class);
		job.setMapOutputValueClass(XmlReducerValueWritable.class);

		job.setMapperClass(XmlExtractor.Map.class);

		job.setReducerClass(XmlExtractor.Reduce.class);

		job.setPartitionerClass(XmlReducerPartitioner.class);
		job.setSortComparatorClass(XmlReducerKeySortComparator.class);
		job.setGroupingComparatorClass(XmlReducerGroupingComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SeqXmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
	}
}
