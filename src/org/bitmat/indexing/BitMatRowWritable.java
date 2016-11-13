package org.bitmat.indexing;
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
	
	public
	BitMatRowWritable() {
		rowId = -1L;
		firstBit = false;
		rowRest = new ArrayList<Long>();
		lastCol = -1L;
	}
	
	public
	BitMatRowWritable(Long id) {
		rowId = id;
		firstBit = false;
		rowRest = new ArrayList<Long>();
		lastCol = -1L;
	}
	
	@SuppressWarnings("unchecked")
	public
	BitMatRowWritable(BitMatRowWritable another) {
		rowId = another.rowId;
		firstBit = another.firstBit;
		rowRest = (ArrayList<Long>) another.rowRest.clone();
		lastCol = another.lastCol;
	}
	
	public 
	void clear() {
		rowId = -1L;
		firstBit = false;
		rowRest = new ArrayList<Long>();
		lastCol = -1L;
	}
	
	public 
	void setId(Long id) {
		rowId = id;
	}
	
	public 
	void addColumn(Long index) {
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
	public 
	void readFields(DataInput in) throws IOException {
		this.clear();
		rowId = in.readLong();
		Integer rowSize = in.readInt();
		firstBit = in.readBoolean();
		while (rowSize-- != 0) rowRest.add(in.readLong());
	}
	
	@Override
	public 
	String toString() {
		String res;
		res = rowId.toString() + " ";
		res += String.valueOf(firstBit) + " ";
		for (Long i:rowRest) {
			res += i.toString() + " ";
		}
		return res;
	}

	@Override
	public 
	void write(DataOutput out) throws IOException {
		this.toString();
		out.writeLong(rowId);
		out.writeInt(rowRest.size());
		out.writeBoolean(firstBit);
		for (Long i:rowRest) out.writeLong(i);
	}
	
}
