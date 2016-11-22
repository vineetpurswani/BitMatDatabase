package org.bitmat.indexing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bitmat.extras.BitMatRowWritable;
import org.bitmat.extras.CompositeKeyWritable;
import org.bitmat.extras.StringSerialization;

public class BitMatIndexingReducer extends Reducer<CompositeKeyWritable, NullWritable, LongWritable, BitMatRowWritable>{
	private MultipleOutputs<LongWritable, BitMatRowWritable> multipleOutputs; 
	
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		Long lastRowId = -1L;
		Long s,p,o;
		BitMatRowWritable bmr = null;
		ArrayList<Boolean> rowVector = new ArrayList<Boolean>();
		ArrayList<Boolean> columnVector = new ArrayList<Boolean>();

		for (@SuppressWarnings("unused") NullWritable value : values) {
			s = key.getSubject();
			p = key.getPredicate();
			o = key.getObject();

			if (lastRowId != s) {
				if (bmr != null) multipleOutputs.write(new LongWritable(p), bmr, "bitmat_"+p);
				else bmr = new BitMatRowWritable();
				bmr.clear();
				bmr.setId(s);
			
				rowVector.addAll(Collections.nCopies((int)(s - lastRowId - 1), false));
				rowVector.add(true);
			}
			lastRowId = s;
			bmr.addColumn(o);
			
			if (!(columnVector.size() > o)) columnVector.addAll(Collections.nCopies((int)(o - columnVector.size() + 1), false));
			columnVector.set(o.intValue(),true);
		}
		if (bmr != null) multipleOutputs.write(new LongWritable(key.getPredicate()), bmr, "bitmat_"+key.getPredicate());
		bmr.clear();
		
		Path metaFile = new Path("/"+FileOutputFormat.getOutputPath(context).getName()+"/bitmat_"+key.getPredicate()+".meta");
		FileSystem fs = metaFile.getFileSystem(context.getConfiguration());
		SequenceFile.Writer mout = new SequenceFile.Writer(fs, context.getConfiguration(), metaFile, Text.class, BytesWritable.class);
		mout.append(new Text("Row Vector"), new BytesWritable(StringSerialization.toByteArray(rowVector)));
		mout.append(new Text("Column Vector"), new BytesWritable(StringSerialization.toByteArray(columnVector)));
		mout.close();
		
//		System.out.println(rowVector);
//		System.out.println(columnVector);
	}

	@Override
	public void setup(Context context){
		multipleOutputs = new MultipleOutputs<LongWritable, BitMatRowWritable>(context);
	}

	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
}
