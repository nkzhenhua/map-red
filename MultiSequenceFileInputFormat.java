import org.apache.hadoop.mapred.MultiFileInputFormat;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.SequenceFileRecordReader;


public class MultiSequenceFileInputFormat <K, V> 
      extends MultiFileInputFormat<K, V>  { 

    @Override
    public RecordReader<K,V> getRecordReader(InputSplit split, JobConf job, 
                             Reporter reporter) throws IOException {
      return new MultiSequenceFileRecordReader(job, (MultiFileSplit)split);
    }

    public static class MultiSequenceFileRecordReader<K, V>
                implements RecordReader<K, V> {
      private MultiFileSplit split;
      private long offset; //total offset read so far;
      private long totLength;
      private FileSystem fs;
      private int count = 0;
      private Path[] paths;
      private JobConf conf;
      
      private SequenceFileRecordReader <K,V>  currentReader;

      private SequenceFileRecordReader <K, V> getSequenceFileRecordReader(JobConf conf, FileSplit fileSplit) 
        throws IOException {
        // doing the job of MapTask::updateJobWithSplit
        conf.set("map.input.file", fileSplit.getPath().toString());
        conf.setLong("map.input.start", fileSplit.getStart());
        conf.setLong("map.input.length", fileSplit.getLength());
        return new SequenceFileRecordReader<K, V>(conf, fileSplit);
      }

      public MultiSequenceFileRecordReader(JobConf conf, MultiFileSplit split)
        throws IOException {
        
        this.split = split;
        fs = FileSystem.get(conf);
        this.paths = split.getPaths();
        this.totLength = split.getLength();
        this.offset = 0;
        this.conf = conf;
        
        currentReader = getSequenceFileRecordReader(conf,
                            new FileSplit(split.getPath(0), 0L, split.getLength(0), conf) );
      }

      @Override
      public void close() throws IOException { }

      @Override
      public long getPos() throws IOException {
        long currentOffset = currentReader == null ? 0 : currentReader.getPos();
        return offset + currentOffset;
      }

      @Override
      public float getProgress() throws IOException {
        return ((float)getPos()) / totLength;
      }

      @Override
      public boolean next(K key, V value) throws IOException {
        if(count >= split.getNumPaths())
          return false;

        /* Read from file, fill in key and value, if we reach the end of file,
         * then open the next file and continue from there until all files are
         * consumed.  
         */
        boolean itemIsRead;
        do {
          itemIsRead = currentReader.next(key, value);
          if( ! itemIsRead ) {
            //close the file
            currentReader.close();
            offset += split.getLength(count);
            
            if(++count >= split.getNumPaths()) //if we are done
              return false;
            
            currentReader = getSequenceFileRecordReader(conf,
                            new FileSplit(split.getPath(count), 0L, split.getLength(count), conf) );
          }
        } while(!itemIsRead) ;
        //update the key and value
        return true;
      }

      @Override
      public K createKey() {
        return currentReader.createKey();
      }

      @Override
      public V createValue() {
        return currentReader.createValue();
      }

    }
}
