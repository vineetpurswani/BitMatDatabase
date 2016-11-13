package org.bitmat.extras;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SecondarySortBasicGroupingComparator extends WritableComparator {
	
	protected SecondarySortBasicGroupingComparator() {
		super(CompositeKeyWritable.class, true);
}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getPredicate().compareTo(key2.getPredicate());
	}
}
