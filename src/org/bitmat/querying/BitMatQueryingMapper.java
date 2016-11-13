package org.bitmat.querying;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.bitmat.extras.BitMatWritable;


public class BitMatQueryingMapper extends Mapper<Text, BitMatWritable, NullWritable, NullWritable> {
		
	@Override
	public void map(Text key, BitMatWritable value, Context context)
			throws IOException, InterruptedException {
		System.out.println(key.toString());
		System.out.println(value.toString());
	}

}
