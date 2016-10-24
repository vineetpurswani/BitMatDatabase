import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BitMatIndexingReducer extends Reducer<CompositeKeyWritable, NullWritable, LongWritable, BitMatRowWritable>{
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		Long lastRowId = -1L;
		BitMatRowWritable bmr = null;
		Long s,p,o;	
		
		for (@SuppressWarnings("unused") NullWritable value : values) {
			s = key.getSubject();
			o = key.getObject();
			
			if (lastRowId != s) {
				if (bmr != null) context.write(new LongWritable(key.getPredicate()), bmr);
				bmr = new BitMatRowWritable(s);
			}
			bmr.addColumn(o);
			lastRowId = s;
		}
		if (bmr != null) context.write(new LongWritable(key.getPredicate()), bmr);
	}
}
