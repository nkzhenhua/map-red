import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings("unchecked")
public class MultipleSequenceFileOutputFormat 	
extends MultipleOutputFormat <WritableComparable, Writable>
{

	SequenceFileOutputFormat<WritableComparable, Writable> theFileOutputFormat;
		
	public MultipleSequenceFileOutputFormat() {
		theFileOutputFormat = null;
	}

	protected RecordWriter<WritableComparable, Writable> 
	getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3)
	throws IOException	    {
		if(theFileOutputFormat == null)
	       theFileOutputFormat = new SequenceFileOutputFormat<WritableComparable, Writable>();
		return theFileOutputFormat.getRecordWriter(fs, job, name, arg3);
	 }

	 protected String generateFileNameForKeyValue(WritableComparable key, Writable value, String name)   {
	    return key.toString() + "-" + name;
	 }

	 protected WritableComparable generateActualKey(WritableComparable key, Writable value)    {
	    return new Text("");
	 }
}
