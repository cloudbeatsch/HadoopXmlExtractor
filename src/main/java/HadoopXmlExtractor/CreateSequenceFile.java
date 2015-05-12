package HadoopXmlExtractor;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class CreateSequenceFile { 
    public static void main( String[] args) throws IOException { 
        if (args.length != 2) {
            System.err.println("Usage: CreateSequenceFile <input directory> <output filename>");
            System.exit(2);
        }

        String uriIn = args[0];
        String uriOut = args[1];
        Configuration conf = new Configuration();
        FileSystem fsIn = FileSystem.get(URI.create(uriIn), conf);
        Path pathIn = new Path(uriIn);
        if (!fsIn.isDirectory(pathIn)) {   
            System.err.println("input path must be a directory");
            System.exit(2);
        }
        FileSystem fsOut = FileSystem.get(URI.create(uriOut), conf);
        Path pathOut = new Path(uriOut);

        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter( fsOut, conf, pathOut, key.getClass(), value.getClass());

        RemoteIterator<LocatedFileStatus> fileIterator = fsIn.listFiles(pathIn, false);

        while (fileIterator.hasNext()){
            Path filePath = fileIterator.next().getPath();
            FSDataInputStream inputStream = inputStream = fsIn.open(filePath);
            try { 
                byte[] buf = new byte[inputStream.available()];
                inputStream.readFully(buf);
                key.set(new Text(UUID.randomUUID().toString()));
                value.set(buf);
                writer.append( key, value);
            } finally{ 
                IOUtils.closeStream(inputStream); 
            }            
        }
        IOUtils.closeStream( writer); 
     } 
}
 
