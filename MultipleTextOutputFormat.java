
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings("unchecked")
public class MultipleTextOutputFormat 
extends MultipleOutputFormat <WritableComparable, Writable>
{

	TextOutputFormat<WritableComparable, Writable> theFileOutputFormat;
	
    public MultipleTextOutputFormat()
    {
        theFileOutputFormat = null;
    }

    protected RecordWriter<WritableComparable, Writable> 
    getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3)
        throws IOException
    {
        if(theFileOutputFormat == null)
            theFileOutputFormat = new TextOutputFormat<WritableComparable, Writable>();
        return theFileOutputFormat.getRecordWriter(fs, job, name, arg3);
    }

    protected String generateFileNameForKeyValue(WritableComparable key, Writable value, String name)
    {
        return key.toString() + "-" + name;
    }

    protected WritableComparable generateActualKey(WritableComparable key, Writable value)
    {
        return new Text("");
    }
   
}
