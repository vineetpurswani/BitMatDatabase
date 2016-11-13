package org.bitmat.querying;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BitMatQueryingReducer extends Reducer<Text, BytesWritable, NullWritable, NullWritable> {

}
