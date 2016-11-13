package org.bitmat.indexing;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class CompositeKeyWritable implements Writable,
WritableComparable<CompositeKeyWritable>{

	private Long s;
	private Long p;
	private Long o;
	
	public CompositeKeyWritable(Long s,Long p,Long o)
	{
		this.p = p;
		this.s = s;
		this.o = o;
	}
	
	public CompositeKeyWritable()
	{
		this(0L,0L,0L);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		s = in.readLong();
		p = in.readLong();
	    o = in.readLong();
	}

	@Override
	public String toString() {
		String res;
		res = s.toString() + " " + p.toString() + " " + o.toString();
		return res;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(s);
		out.writeLong(p);
		out.writeLong(o);
	}

	@Override
	public int compareTo(CompositeKeyWritable key) {
		int result = p.compareTo(key.p);
		if(result == 0)
		{
			result = s.compareTo(key.s);
			if(result == 0)
				result = o.compareTo(key.o);
		}
		return result;
	}
	
	public Long getPredicate()
	{
		return p;
	}
	
	public Long getSubject()
	{
		return s;
	}
	
	public Long getObject()
	{
		return o;
	}

}
