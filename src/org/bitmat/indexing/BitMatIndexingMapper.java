package org.bitmat.indexing;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BitMatIndexingMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable>  {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] ntriple = value.toString().split("\\s+");
		context.write(new CompositeKeyWritable(Long.parseLong(ntriple[0]),Long.parseLong(ntriple[1]),Long.parseLong(ntriple[2])),NullWritable.get());
	}
}
