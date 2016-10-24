import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BitMatIndexingMain extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf("Two parameters are required for BitMatIndexingMain - <input dir> <output dir> <number of predicates>\n");
			return -1;
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJobName("BitMatIndexing");

		job.setJarByClass(BitMatIndexingMain.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BitMatIndexingMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(SecondarySortBasicPartitioner.class);
		job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
		job.setReducerClass(BitMatIndexingReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BitMatRowWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(new Integer(args[2]));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new BitMatIndexingMain(), args);
		System.exit(exitCode);
}
}
