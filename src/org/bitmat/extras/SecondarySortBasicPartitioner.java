package org.bitmat.extras;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortBasicPartitioner  extends Partitioner<CompositeKeyWritable, NullWritable> {
	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value,
			int numReduceTasks) {

		return (key.getPredicate().hashCode() % numReduceTasks);
}
}
