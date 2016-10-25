import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BitMatIndexingMain extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Two parameters are required for BitMatIndexingMain - <input dir> <output dir>\n");
			return -1;
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJobName("BitMatIndexing");

		Path outputFilePath = new Path(args[1]);
		
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
		
		/*
		 * Using MultipleOutputs creates zero-sized default output Ex:
		 * part-r-00000. To prevent this use LazyOutputFormat
		 */
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "SEQ", SequenceFileOutputFormat.class, LongWritable.class, BitMatRowWritable.class);
		
		/* This line is to accept input recursively */
		// FileInputFormat.setInputDirRecursive(job, true);
		
		/*
		 * Delete output filepath if already exists
		 */
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		// job.setNumReduceTasks(new Integer(args[2]));
		job.setNumReduceTasks(8);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new BitMatIndexingMain(), args);
		System.exit(exitCode);
	}
}
