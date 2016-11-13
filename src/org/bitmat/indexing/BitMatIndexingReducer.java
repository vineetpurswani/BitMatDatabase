package org.bitmat.indexing;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BitMatIndexingReducer extends Reducer<CompositeKeyWritable, NullWritable, LongWritable, BitMatRowWritable>{
	private MultipleOutputs<LongWritable, BitMatRowWritable> multipleOutputs; 
	
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		Long lastRowId = -1L;
		BitMatRowWritable bmr = null;
		Long s,p,o;	
		
		for (@SuppressWarnings("unused") NullWritable value : values) {
			s = key.getSubject();
			p = key.getPredicate();
			o = key.getObject();
			
			if (lastRowId != s) {
				if (bmr != null) multipleOutputs.write(new LongWritable(p), bmr, "bitmat_"+p);
				else bmr = new BitMatRowWritable();
				bmr.clear();
				bmr.setId(s);
			}
			bmr.addColumn(o);
			lastRowId = s;
		}
		if (bmr != null) multipleOutputs.write(new LongWritable(key.getPredicate()), bmr, "bitmat_"+key.getPredicate());
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
