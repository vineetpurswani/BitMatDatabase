import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
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
		rowRest = new ArrayList<Long>();
		try {
			firstBit = in.readBoolean();
			while (true) {
				rowRest.add(in.readLong());
			}
		}
		catch (EOFException e) {
			e.printStackTrace();
		}
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
		out.writeBoolean(firstBit);
		for (Long i:rowRest) {
			out.writeLong(i);
		}
	}
	
}
