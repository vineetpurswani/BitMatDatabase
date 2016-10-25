import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class BitMatRowWritable implements Writable {

	private Long rowId;
	private boolean firstBit;
	private ArrayList<Long> rowRest;
	private Long lastCol;
	
	BitMatRowWritable() {
		firstBit = false;
		rowRest = new ArrayList<Long>();
		lastCol = -1L;
	}
	
	BitMatRowWritable(Long id) {
		rowId = id;
		firstBit = false;
		rowRest = new ArrayList<Long>();
		lastCol = -1L;
	}
	
	public void addColumn(Long index) {
		if (index == lastCol+1) {
			if (index == 0) {
				firstBit = true;
				rowRest.add(1L);
			}
			else {
				rowRest.set(rowRest.size()-1, rowRest.get(rowRest.size()-1)+1);
			}
		}
		else {
			rowRest.add(index - lastCol - 1);
			rowRest.add(1L);
		}
		lastCol = index;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Long rowId = in.readLong();
		Integer rowSize = in.readInt();
		rowRest = new ArrayList<Long>();
		firstBit = in.readBoolean();
		while (rowSize-- != 0) rowRest.add(in.readLong());
	}
	
	@Override
	public String toString() {
		String res;
		res = rowId.toString() + " ";
		res += String.valueOf(firstBit) + " ";
		for (Long i:rowRest) {
			res += i.toString() + " ";
		}
		return res;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(rowId);
		out.writeInt(rowRest.size());
		out.writeBoolean(firstBit);
		for (Long i:rowRest) out.writeLong(i);
	}
	
}
