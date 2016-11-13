package org.bitmat.querying;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.bitmat.indexing.BitMatRowWritable;


public class BitMatWritable implements Writable {
	private ArrayList<BitMatRowWritable> rows;
	private Long matId;

	BitMatWritable() {
		rows = new ArrayList<BitMatRowWritable>();
		matId = -1L;
	}

	public void setId(Long id) {
		matId = id;
	}

	public void addRow(BitMatRowWritable row) {
		rows.add(new BitMatRowWritable(row));
	}

	@Override
	public String toString() {
		String res = matId.toString()+"\n";
		for (BitMatRowWritable i : rows) {
			res += i.toString()+"\n";
		}
		return res;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

}
