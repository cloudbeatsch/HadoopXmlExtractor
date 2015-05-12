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

public class SeqXmlRecordReader extends RecordReader<Text, XmlMapperValueWritable> {
	
	private static final int SEARCH_FOR_START_TAG = -1;
	private static final int IGNORE_TAG = -2;
	private static final int END_OF_STREAM = -3;
	private static final int FOUND_START_TAGS = -4;
	
	private SequenceFile.Reader reader = null;
	FileSplit fileSplit = null;
	private Text seqKey = new Text();
	private Text seqValue = new Text();
	private Text key = new Text();
	private XmlMapperValueWritable value = new XmlMapperValueWritable();
	private Text xmlStream = new Text();
	private DataOutputBuffer buffer = null;

	
	private byte[][] startTags;
	private byte[][] endTags;
	private byte[][] bufferAdditions;
	private byte[][] requiredAttributes;
	private XPathInfoWritable[][] xPathExpressions;

	private String sortOrderDelim;

	private long fileLength = 0;
	private long currentPosition = 0;
	static String grtChar = ">";
	static byte[] grtCharBytes;
	static int grtCharBytesLength; 
	ReadMatchToken token = null ;

	private XPathInfoWritable[] parseNodeValues(String nodeString, String delim, String[] nodeValues) {
		final int NODE_NAME_POS = 0;
		final int NODE_HASATTRIBUTE_POS = 1;
		final int NODE_GETCHILDREN_POS = 2;
		final int NODE_ATTRIBUTE_POS = 3;
		String[] node = nodeString.split(delim);

		// creating the start tag
		StringBuilder startTag = new StringBuilder();
		startTag.append("<").append(node[NODE_NAME_POS]);
		// true means that the node has attributes and we look only for
		// "<NODENAME "
		// instead of "<NODENAME>"
		if (node[NODE_HASATTRIBUTE_POS].equalsIgnoreCase("true")) {
			startTag.append(" ");
		} else {
			startTag.append(">");
		}

		StringBuilder endTag = new StringBuilder();
		StringBuilder bufferAddition = new StringBuilder();

		// false means that we're only interested in the start tag but
		// not the children
		// that's why we add a bufferAddition to fake the end tag
		if (node[NODE_GETCHILDREN_POS].equalsIgnoreCase("false")) {
			endTag.append(">");
			bufferAddition.append("</").append(node[NODE_NAME_POS]).append(">");
		} else {
			endTag.append("</").append(node[NODE_NAME_POS]).append(">");
		}
		nodeValues[0] = startTag.toString();
		nodeValues[1] = endTag.toString();
		nodeValues[2] = bufferAddition.toString();
		nodeValues[3] = node[NODE_ATTRIBUTE_POS].trim();
		

		XPathInfoWritable[] expressions = new XPathInfoWritable[node.length - NODE_ATTRIBUTE_POS - 1];
		int j=0;
		for (int i=(NODE_ATTRIBUTE_POS+1); i< node.length; i++) {
			String[] expressionSplit = node[i].split(sortOrderDelim);
			expressions[j++] = new XPathInfoWritable(Integer.parseInt(expressionSplit[0]), expressionSplit[1]);
		}
		return expressions;
	}

	private void createParsingArrays(Configuration conf)
			throws IOException, InterruptedException {

		String delim = conf.get(XmlExtractor.DELIMITOR_STRING_KEY);
		String nodesString = conf.get(XmlExtractor.PARSING_NODES_KEY);
		sortOrderDelim = conf.get(XmlExtractor.SORT_ORDER_DELIMITOR_KEY);

		String[] nodes = nodesString.split(delim);
		int nrOfNodes = nodes.length;
		startTags = new byte[nrOfNodes][];
		endTags = new byte[nrOfNodes][];
		bufferAdditions = new byte[nrOfNodes][];
		requiredAttributes = new byte[nrOfNodes][];
		xPathExpressions = new XPathInfoWritable[nrOfNodes][];

		for (int i = 0; i < nodes.length; i++) {
			String[] nodeValues = new String[4];
			xPathExpressions[i] = parseNodeValues(conf.get(nodes[i]), delim, nodeValues);

			startTags[i] = nodeValues[0].getBytes("utf-8");
			endTags[i] = nodeValues[1].getBytes("utf-8");
			bufferAdditions[i] = nodeValues[2].getBytes("utf-8");
			requiredAttributes[i] = nodeValues[3].getBytes("utf-8");
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException, UnsupportedEncodingException{
		Configuration conf = context.getConfiguration();
		createParsingArrays(conf);
		
		grtCharBytes = grtChar.getBytes("utf-8");
		grtCharBytesLength = grtCharBytes.length;
		
		fileSplit = (FileSplit) split;
		Path file = fileSplit.getPath();
		fileLength = fileSplit.getLength();
		FileSystem fs = file.getFileSystem(conf);
		
		reader = new SequenceFile.Reader(fs, file, conf);
		buffer = new DataOutputBuffer();
		token = null;
	}
	
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Stack<Integer> matchIndexes = new Stack<Integer>();

		try {
			do {
				if (token == null){
					if (reader.next(seqKey, seqValue)) {
						currentPosition = reader.getPosition();
						token = new ReadMatchToken();
						key.set(new Text(UUID.randomUUID().toString()));
					}
					else {
						return false;
					}
				}
				do {
					token.index = SEARCH_FOR_START_TAG;
					buffer.reset();
					matchIndexes.clear();
					token = readUntilMatch(token, startTags, matchIndexes);
					if (token.index == FOUND_START_TAGS) {
						token = readUntilMatch(token, endTags, matchIndexes);
						if (token.index >= 0) {
							xmlStream.set(startTags[token.index], 0, startTags[token.index].length);
							xmlStream.append(buffer.getData(), 0, buffer.getLength());
							xmlStream.append(bufferAdditions[token.index], 0, bufferAdditions[token.index].length);
							value.setXmlStream(xmlStream);
							value.setSequence(token.currentPos);
							value.setXPathExpressions(xPathExpressions[token.index]);
							return true;
						}
					}
				} while (token.index != END_OF_STREAM);
				token = null;
			} while (true);
		}
		catch (EOFException ex){
			reader.close();
		}
		token = null;
		return false;
	}
	
	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}
				
	@Override
	public XmlMapperValueWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public float getProgress() throws IOException {
		return currentPosition / fileLength;				
	}	
	
	private class ReadMatchToken {
		int currentPos = 0;
		Boolean isInStartScan = true;
		int index = SEARCH_FOR_START_TAG;
	}
	
	private ReadMatchToken readUntilMatch(ReadMatchToken token, byte[][] matchArray, Stack<Integer> matchIndex)
			throws IOException {
		int[] positions = new int[matchArray.length];
		int[] attrPosition = new int[matchArray.length];
		boolean[] foundAttribute = new boolean[matchArray.length];
		boolean ignoreTag = false;
		
		int grtCharPosition = 0;
		
		byte[] bytes = seqValue.getBytes();
		// bytes are only valid till seqValue.getLength()
		while (token.currentPos< seqValue.getLength()){
			int b = bytes[token.currentPos++];
			
			// check if we already have matches, so looking for the attributes and closing tags
			if (!matchIndex.empty()) {
				// save to buffer:
				buffer.write(b);
				if (token.isInStartScan) {
					if (b == grtCharBytes[grtCharPosition]) {
						grtCharPosition++;
						if(grtCharPosition >= grtCharBytesLength){
							token.isInStartScan = false;
						}
					}
					else {
						for (int index : matchIndex){
							// check if we require an attribute as part of the start
							// tag
							if (!foundAttribute[index] && requiredAttributes[index].length > 0) {
								if (b == requiredAttributes[index][attrPosition[index]]) {
									attrPosition[index]++;
									if (attrPosition[index] >= requiredAttributes[index].length) {
										foundAttribute[index] = true;
										attrPosition[index] = 0;
									}
								} else {
									attrPosition[index] = 0;
								}
							}		
						}
					}
				}
				// check if we're matching. since we're looking for closing tag (which is the same for :
				// all elements in the matchIndex, we compare only against one 
				int oneIndex = matchIndex.peek();
				if (b == matchArray[oneIndex][positions[oneIndex]]) {
					positions[oneIndex]++;
					if (positions[oneIndex] >= matchArray[oneIndex].length) {
						// we matched
						for (int index : matchIndex){
							if (requiredAttributes[index].length > 0) {
								if (foundAttribute[index]) {
									token.index = index;
									return token;
								} else {
									ignoreTag = true;
								}
							} else {
								token.index = index;
								return token;
							}
						}
					} 
				}
				else {
					positions[oneIndex] = 0;
				}

				if (ignoreTag){
					token.index = IGNORE_TAG;
					return token;
				}
			
			} else {
				for (int index = 0; index < matchArray.length; index++) {
					// check if we're matching:
					if (b == matchArray[index][positions[index]]) {
						positions[index]++;
						if (positions[index] >= matchArray[index].length) {
							matchIndex.push(index);
						}
					} else {
						positions[index] = 0;
					}
				}
				if (!matchIndex.empty()) {
					token.index = FOUND_START_TAGS;
					token.isInStartScan = true;
					return token;
				}
			}
		}
		token.index = END_OF_STREAM;
		return token;
	}

}