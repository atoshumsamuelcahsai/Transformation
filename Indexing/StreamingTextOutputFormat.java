package PIT.Indexing;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamingTextOutputFormat<K, V> extends TextOutputFormat<K, V> 

{
	protected static class StreamingLineRecordWriter<K, V> implements RecordWriter<K, V> 
	{
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;

		static 
		{
			try 
			{
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee)
			{
				throw new IllegalArgumentException("can't find " + utf8+ " encoding");
			}
		}

		protected DataOutputStream out;
		private final byte[] keyValueSeparator;
		private final byte[] valueDelimiter;
		private boolean dataWritten = false;

		public StreamingLineRecordWriter(DataOutputStream out, String keyValueSeparator, String valueDelimiter) 
		{
			this.out = out;
			try 
			{
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
				this.valueDelimiter = valueDelimiter.getBytes(utf8);
			} catch (UnsupportedEncodingException uee) 
			{
				throw new IllegalArgumentException("can't find " + utf8  + " encoding");
			}
		}  
		
		public StreamingLineRecordWriter(DataOutputStream out) 
		{
            this(out, "\t", ",");
         }
		
		
		 /**
         * Write the object to the byte stream, handling Text as a special case.
         *
         * @param o
         *            the object to print
         * @throws IOException
         *             if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException 
        {
                if (o instanceof Text)
                {
                        Text to = (Text) o;
                        out.write(to.getBytes(), 0, to.getLength());
                }
                else 
                {
                        out.write(o.toString().getBytes(utf8));
                }
        }
        
        public synchronized void write(K key, V value) throws IOException {
        	 
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                    return;
            }

            if (!nullKey) {
                    // if we've written data before, append a new line
                    if (dataWritten) 
                    {
                            out.write(newline);
                    }

                    // write out the key and separator
                    writeObject(key);
                    out.write(keyValueSeparator);
            } else if (!nullValue) {
                    // write out the value delimiter
                    out.write(valueDelimiter);
            }

            // write out the value
            writeObject(value);

            // track that we've written some data
            dataWritten = true;
    }


        public synchronized void close(Reporter reporter) throws IOException 
        {
            // if we've written out any data, append a closing newline
            if (dataWritten) 
            {
                    out.write(newline);
            }

            out.close();
    }
	}

	  @Override
      public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem,
                      JobConf job, String name, Progressable progress) throws IOException {
              boolean isCompressed = getCompressOutput(job);
              String keyValueSeparator = job.get("mapred.textoutputformat.separator",
                              "\t");
              String valueDelimiter = job.get("mapred.textoutputformat.delimiter",
                              "\t");
              if (!isCompressed) {
                      Path file = FileOutputFormat.getTaskOutputPath(job, name);
                      FileSystem fs = file.getFileSystem(job);
                      FSDataOutputStream fileOut = fs.create(file, progress);
                      return new StreamingLineRecordWriter<K, V>(fileOut,
                                      keyValueSeparator, valueDelimiter);
              } else {
                      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
                                      job, GzipCodec.class);
                      // create the named codec
                      CompressionCodec codec = ReflectionUtils.newInstance(codecClass,
                                      job);
                      // build the filename including the extension
                      Path file = FileOutputFormat.getTaskOutputPath(job,
                                      name + codec.getDefaultExtension());
                      FileSystem fs = file.getFileSystem(job);
                      FSDataOutputStream fileOut = fs.create(file, progress);
                      return new StreamingLineRecordWriter<K, V>(new DataOutputStream(
                                      codec.createOutputStream(fileOut)), keyValueSeparator,
                                      valueDelimiter);
              }
      }
		 
	


}
