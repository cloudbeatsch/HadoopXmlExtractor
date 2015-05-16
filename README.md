#HadoopXmlExtractor - Efficiently extract data from XML files stored in Hadoop HDFS sequence files#

To avoid the performance penalty of loading or parsing the complete XML document, we created a custom record reader that implements a scanner to search the raw byte stream for specific xml nodes to extract. These extracted nodes provide the input data for the map job, which then loads the XML fragment into a DOM and runs XPath expressions against it. This allows for an efficient extraction while still providing a standard way to select the nodes of interest (using XPath). 
For instance a data scientist wants to analyze the performance of a particular control unit (e.g. the thermostat) across the last 4 years. He would define an extraction rule for all thermostats and run the job against the last 4 years sequence files. The output of this job contains all the thermostat data including additional metadata such as timestamps, location or device identification.

## Pre-requisites: ##
- Maven: [http://maven.apache.org/](http://maven.apache.org/)
- Hadoop cluster, Horton Sandbox or HDInsight Emulator

## Run examples on Windows - using HDInsight Emulator ##
1.) Download and install the HDInsight Emulator [http://azure.microsoft.com/en-us/documentation/articles/hdinsight-hadoop-emulator-get-started/](http://azure.microsoft.com/en-us/documentation/articles/hdinsight-hadoop-emulator-get-started/)

2.) Start the Hadoop local services

3.) Open the Hadoop command line

If this is the first time you use the Emulator you have to start it in Admin mode and run the following 3 commands:

    # Add a user group called hdfs  
    net localgroup hdfs /add
    
    # Adds a user called hadoop to the group
    net localgroup hdfs hadoop /add
    
    # Adds the local user to the group (your login user)
    net localgroup hdfs <username> /add


4.) Change the directory to your repo and run .\scripts\run.bat

The script creates the respective hdfs directories, copies the sample xml files, builds the project, runs two sample extractions and displays the extraction results.

## Run examples on Linux – using Horton Sandbox##
1.) Download the [Horton Sandbox 2.2](http://hortonworks.com/products/hortonworks-sandbox/) – add to HyperV – then run 

2.) Get the Hadoop bundle tar file
 
	http://mirror.cogentco.com/pub/apache/hadoop/common/hadoop-2.5.2/hadoop-2.5.2.tar.gz


3.) Extract that tar file to the ./lib directory after the git pull

4.) Compile the files into a JAR file 

5.) You can download Intellij IDEA Community edition and compile/pack to jar 

	a.) Download and install https://www.jetbrains.com/idea/download/ 
	b.) Create a new project 
	c.) Set project structure adding references to the following from the Apache Hadoop lib
			commons-cli-1.2.jar
			hadoop-common-2.5.2.jar
			hadoop-hdfs-2.5.2.jar
			hadoop-mapreduce-client-app-2.5.2.jar
			hadoop-mapreduce-client-common-2.5.2.jar
			hadoop-mapreduce-client-core-2.5.2.jar
			hadoop-mapreduce-client-hs-2.5.2.jar
			hadoop-mapreduce-client-jobclient-2.5.2.jar
	d.) Create a build artifact to generate the JAR file and include dependencies 
6.) ‘scp’ the files using Putty or similar tools to a directory on the VM Guest 

	a.) Copy the entire directory structure
		i.) pscp -r <source> <target>
		ii.) where target is "root@x.x.x.x:~/xmlextract

7.) Then, run the ../scripts/run.sh  

## Implementation ##
The class `XmlExtractor` implements the extraction job and takes the following three arguments to run:

- The input path which contains the compressed sequence files (usually one sequence file contains multiple xml files)
- The path where the extracted elements are stored
- The path to the extraction configuration file


The extraction job reads the sequence files using a custom RecordReader called `SeqXmlRecordReader`. It reads each xml file as a binary stream and scans it for the requested xml elements. Once a start tag is found, it scans for the corresponding end tag and provides the containing stream (including start and end tags) as the input to the Mapper (if we’re only interested in attributes, we omit the element content and add a closing tag instead). This reader emits values of type `XmlMapperValueWritable`, which contains the extracted xml stream, a list of XPath expression the Mapper has to run against that stream as well as information about the current stream position and the final output column number. 
The Mapper loads the stream into a DOM and runs the configured XPath expressions against it. For each expression which returns nodes, we create the Mapper output key/value pairs (which are of type `XmlReducerKeyWritable` and `XmlReducerValueWritable` respectively):


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

The output key consists of the triple of file guid, the sequence number and the column order. The output value contains the result of the XPath query and the corresponding column order.
 
Between the Mapper output and the Reducer input, the shuffling takes place:

- The custom grouping comparator `XmlReducerGroupingComparator` ensures that all records containing the same guid (records from the same xml file) will be processed by the same Reducer. This allows us to correlated different query results into one final row.
- The custom sort comparator `XmlReducerKeySortComparator` sorts the records by the input file, the stream position and the column number. This ensures that we assign columns in the same sequence as the file has been processed.

Using the above grouping and sorting, the Reducer can simply iterate across all values and build the output row. This works because all values with the same key are extracted from the same file and sorted in their sequence and their column order:

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

## Extraction configuration file ##
Configuration is done using Hadoop resource configuration files. Below an example:

	<?xml version="1.0"?>
	<?xml-stylesheet type = "text/xsl" href="configuration.xsl"?>
	<configuration>
	  <property>
	    <name>xmlextractor.delimiter_string</name>
	    <value>;</value>
	  </property>
	  <property>
	    <name>xmlextractor.sort_order_delimiter_string</name>
	    <value>#</value>
	  </property>
	  <property>
	    <name>xmlextractor.output_delimiter_string</name>
	    <value>;</value>
	  </property>
	  <property>
	    <name>xmlextractor.nodes</name>
	    <value>store;address;inventory;book;</value>
	  </property> 
	  <property>
	    <name>xmlextractor.nr_of_columns</name>
	    <value>6</value>
	  </property>
	
	  <property>
	    <name>store</name>
	    <value>store;true;false; ;0#//store/@name;</value>
	  </property> 
	  <property>
	    <name>address</name>
	    <value>address;false;true; ;1#//address/phone/text();</value>
	  </property> 
	  <property>
	    <name>inventory</name>
	    <value>inventory;true;false; ;2#//inventory/@month;3#//inventory/@day;</value>
	  </property> 
	  <property>
	    <name>book</name>
	    <value>book;true;false;bk106;4#//book/@id;5#//book/@inStock;</value>
	  </property> 
	</configuration>


- **`xmlextractor.delimiter_string`** specifies the delimiter used within the configuration file
- **`xmlextractor.sort_order_delimiter_string`** specifies the used for separating the sort order
- **`xmlextractor.output_delimiter_string`** specifies the delimiter used for the job output
- **`xmlextractor.nodes lists`** the names of the extraction properties separated by the string defined as delimiter_string
- **`xmlextractor.nr_of_columns`** specifies the number of columns per row

Extraction properties are XML elements that consist of a name and a value:

		  <property>
		    <name>store</name>
		    <value>store;true;false; ;0#//store/@name;</value>
		  </property> 
 The values are strings and delimited with the value specified as delimiter_string (default ";"). Each property defines an element that should be extracted and send to the mapper. The mapper loads the extracted xml stream into a DOM and applies the specified XPath expressions. The results are sorted using the stream sequence and the sort order and correlated into rows during reduce.

	  <property>
	    <name>xmlparser.parsing_nodes</name>
	    <value>store;address;inventory;book;</value>
	  </property>

In the above example, the extraction job will look up the following properties for xml extraction: 

- store
- address
- inventory
- book
	
Each of these properties define one XML element that will be extracted and sent to the mapper.  
Defining a property:

	<property>
    	<name>PROPERTY_NAME</name>
		<value>ELEMENT_NAME;HAS_ATTRIBUTE;INCLUDE_CHILDREN;ATTRIBUTE_VALUE;SORT_ORDER1#XPATH1;SORT_ORDER2#XPATH2;...;SORT_ORDERn#XPATH;</value>
	</property>

- **PROPERTY_NAME**: name that will be referred in "xmlparser.parsing_nodes"
- **ELEMENT_NAME**: name of the xml element that will be extracted
- **HAS_ATTRIBUTE**: defines whether the element contains attributes. If there is no attribute, we will search for the following start string "<ELEMENT_NAME>", if the ELEMENT_NAME contains attributes, the start string "<ELEMENT_NAME"
- **INCLUDE_CHILDREN**: If set to true, the node including its children will be extracted. If false, only the start tag will be extracted.
- **ATTRIBUTE_VALUE**: Attribute search term. The element will only be extracted if the the search term is part of the start tag. If the search term can't be found, the node will be ignored. IMPORTANT: if there is no need for an ATTRIBUTE_VALUE, the value has to be a space (and not empty):

	e.g: <value>header;false;true; ;0#//header/ownerId/text();</value>

- The **SORT_ORDER#XPATH** pairs define the semicolon separated output. SORT_ORDER defines the order (lowest value means most left) and XPATH defines the XPath expression which will be applied to the extracted element. Hadoop shuffles the mapper outputs and sorts them according to the SORT_ORDER. The reducer output are the results of the executed XPATH expression, sorted by SORT_ORDER and separated by a semicolon. Note that the SORT_ORDER is not defining the absolute position but the relative position among all extracted expressions.

The examples below refer to the following sample XML doc:

	<dataIngest senderId ="6a2c806e-a3aa-4749-9f23-bbd905d9d22f" status="done">
 		<header>
    			<specNr>10.65.107.2</specNr>
    			<ownerId>5544778899ZZXXX54887U</ownerId>>
    			<systemVersion>3.42.30.10860</systemVersion>
  		</header>
  		...
  		<device deviceId="thermostat">
          	<data>
            	<info>X5466</info>
				<max>35</max>
				<min>3</min>
         	 </data>
        </device>
	</dataIngest>

`xmlparser.nodes` specifies the list of extraction properties:

		  <property>
		    <name>xmlparser.parsing_nodes</name>
		    <value>dataIngest;headerNode;thermostat;</value>
		  </property>


**Example 1 - Extracting an attribute value without reading the whole element.** The sort order (column) of the queried value will be 2:

      <property>
    	<name>dataIngest</name>
    	<value>dataIngest;true;false; ;2#//dataIngest/@senderID;</value>
      </property> 

**Example 2 - Extracting a node and query 2 values (ownerId (0), specNr (1)):**

	  <property>
	  	<name>headerNode</name>
		<value>header;false;true; ;0#//header/onwerId/text();1#//header/specNr/text</value>
	  </property> 

**Example 3** - Extracting a node but only if a specific attribute value (XYZ_DEVICE) has been set.** The sort order will be 3:

	  <property>
	    <name>thermostat</name>
	    <value>device;true;true;thermostat;3#//device/data/info/text();</value>
	  </property> 

