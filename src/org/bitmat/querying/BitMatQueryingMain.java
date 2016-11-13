package org.bitmat.querying;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BitMatQueryingMain extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Two parameters are required for BitMatIndexingMain - <input dir> <output dir>\n");
			return -1;
		}
		Path outputFilePath = new Path(args[1]);
		Job job = new Job(getConf(), "Map-Only Job");
		job.setJarByClass(BitMatQueryingMain.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		/*
		 * Set no of reducers to 0
		 */
		job.setNumReduceTasks(0);

		job.setMapperClass(BitMatQueryingMapper.class);
		job.setInputFormatClass(WholeFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		boolean success = job.waitForCompletion(true);
		return(success ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new BitMatQueryingMain(), args);
		System.exit(exitCode);
	}
	
}
