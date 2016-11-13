package org.bitmat.indexing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BitMatIndexingReducer extends Reducer<CompositeKeyWritable, NullWritable, LongWritable, BitMatRowWritable>{
	private MultipleOutputs<LongWritable, BitMatRowWritable> multipleOutputs; 
	
	private Long addColumn(ArrayList<Long> row, Long index, Long lastCol) {
		if (index == lastCol+1) {
			if (index == 0) {
				row.add(1L);
			}
			else {
				row.set(row.size()-1, row.get(row.size()-1)+1);
			}
		}
		else {
			row.add(index - lastCol - 1);
			row.add(1L);
		}
		return index;
	}
	
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		Long lastRowId = -1L, lastCol = -1L;
		Long s,p,o;
		ArrayList<Long> row = new ArrayList<Long>();
		boolean firstBit = false;
		BitMatRowWritable bmr = null;
		
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
			lastRowId = s;
			lastCol = addColumn(row, o, lastCol);
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
