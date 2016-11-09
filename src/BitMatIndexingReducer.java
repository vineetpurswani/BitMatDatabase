import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BitMatIndexingReducer extends Reducer<CompositeKeyWritable, NullWritable, NullWritable, BytesWritable>{
	private MultipleOutputs<NullWritable, BytesWritable> multipleOutputs; 
	
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
	
	private void writeRow(Long bitmatId, Long rowId, boolean firstBit, ArrayList<Long> row) throws IOException, InterruptedException {
		ByteBuffer buffer = ByteBuffer.allocate((17+row.size()*8));
		buffer.putLong(rowId);
		buffer.putLong(row.size()*8L);
		buffer.put((byte)(firstBit?1:0));
		for (Long i:row) { buffer.putLong(i); }
		multipleOutputs.write(NullWritable.get(), new BytesWritable(buffer.array()), "bitmat_"+bitmatId);
	}
	
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		Long lastRowId = -1L, lastCol = -1L;
		Long s,p,o;
		ArrayList<Long> row = new ArrayList<Long>();
		boolean firstBit = false;
		
		for (@SuppressWarnings("unused") NullWritable value : values) {
			s = key.getSubject();
			p = key.getPredicate();
			o = key.getObject();
			
			if (o == 0) firstBit = true;
			if (lastRowId != s && row.size() != 0) {
				writeRow(p, s, firstBit, row);
				row.clear();
				firstBit = false;
				lastCol = -1L;
			}
			lastRowId = s;
			lastCol = addColumn(row, o, lastCol);
		}
		if (row.size()!=0) writeRow(key.getPredicate(), key.getSubject(), firstBit, row);
	}
	
	@Override
	public void setup(Context context){
		multipleOutputs = new MultipleOutputs<NullWritable, BytesWritable>(context);
	}
	
	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
}
