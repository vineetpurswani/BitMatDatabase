package org.bitmat.querying;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bitmat.indexing.BitMatRowWritable;


public class WholeFileRecordReader extends RecordReader<Text, BitMatWritable> {
	private FileSplit fileSplit;
	private Configuration conf;
	private BitMatWritable value = new BitMatWritable();
	private boolean processed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

			try {
				WritableComparable nul = (WritableComparable) reader.getKeyClass().newInstance();
				Writable val = (Writable) reader.getValueClass().newInstance();
				while (reader.next(nul,val)) {
					value.addRow((BitMatRowWritable) val);  
				}
				value.setId(((LongWritable)nul).get());
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}

			reader.close();
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(fileSplit.getPath().toString());
	}

	@Override
	public BitMatWritable getCurrentValue() throws IOException,
	InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

}
