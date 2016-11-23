package org.bitmat.extras;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class BitMatRowWritable implements Writable {

	public Long rowId;
	public boolean firstBit;
	public ArrayList<Long> rowRest;
	public Long lastCol;
	
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
	BitMatRowWritable(Long id, ArrayList<Boolean> rowList) {
		rowRest = new ArrayList<Long>();
		rowId = id;
		firstBit = rowList.get(0);
		boolean curBit = firstBit;
		long curCount = 0L;
		for (Boolean b : rowList) {
			if (b ^ curBit == false) curCount++;
			else {
				rowRest.add(curCount);
				curBit ^= true;
				curCount = 1L;
			}
		}
		rowRest.add(curCount);
		lastCol = rowList.size()-1L;
	}
	
	public
	boolean[] toBoolArray() {
		int sum = 0;
		for (Long l : rowRest) sum += l.intValue();
		boolean[] arr = new boolean[sum];
		int curi = 0; boolean curb = firstBit; long curn = rowRest.get(curi); 
		for (int i=0; i<sum; i++, curn--) {
			if (curn==0) {
				curn = rowRest.get(++curi);
				curb ^= true;
			}
			arr[i] = curb;
		}
		return arr;
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
	
	public
	void unFold(BitMatRowWritable bitmask) {
		if (rowRest==null || rowRest.size() == 0) {
			lastCol = bitmask.lastCol;
			firstBit = bitmask.firstBit;
			rowRest = (ArrayList<Long>) bitmask.rowRest.clone();
			return;
		}
		
		ArrayList<Long> trowRest = rowRest;
		rowRest = new ArrayList<Long>();
		boolean tCurBit = firstBit, bmCurBit = bitmask.firstBit, nCurBit;
		int ti=0, bmi=0;
		long tn=trowRest.get(ti), bmn=bitmask.rowRest.get(bmi);
		
		lastCol = lastCol > bitmask.lastCol ? lastCol : bitmask.lastCol;
		firstBit = firstBit & bitmask.firstBit;
		nCurBit = !firstBit;
		while (tn > 0 && bmn > 0) {
			long t = 0L;
			if (tCurBit & bmCurBit) t = tn<bmn ? tn : bmn;
			else {
				if (tCurBit) t = bmn;
				else if (bmCurBit) t = tn;
				else t = tn>bmn ? tn : bmn;
			}
			
			if (nCurBit != (tCurBit & bmCurBit)) rowRest.add(t);
			else rowRest.set(rowRest.size()-1, rowRest.get(rowRest.size()-1) + t);
			nCurBit = tCurBit & bmCurBit;

			tn -= t; bmn -= t;
			while (ti < trowRest.size()-1 && tn <= 0) {
				tn += trowRest.get(++ti);
				tCurBit ^= true;
			}
			while (bmi < bitmask.rowRest.size()-1 && bmn <= 0) {
				bmn += bitmask.rowRest.get(++bmi);
				bmCurBit ^= true;
			}
		}
		
//		if (bmi == bitmask.rowRest.size()-1) return;
//		else if (ti == trowRest.size()-1) {
//			if (nCurBit == bmCurBit) {
//				rowRest.set(rowRest.size()-1, rowRest.get(rowRest.size()-1) + bmn);
//				bmi++;
//			}
//			while (bmi < bitmask.rowRest.size()) {
//				rowRest.add(bitmask.rowRest.get(bmi++));
//			}
//		}
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
//		System.out.println(this.toString());
		this.toString();
		out.writeLong(rowId);
		out.writeInt(rowRest.size());
		out.writeBoolean(firstBit);
		for (Long i:rowRest) out.writeLong(i);
	}
	
}
