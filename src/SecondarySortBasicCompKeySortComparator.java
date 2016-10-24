import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SecondarySortBasicCompKeySortComparator extends WritableComparator  {
	
	protected SecondarySortBasicCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getPredicate().compareTo(key2.getPredicate());
		if (cmpResult == 0)// same Predicate
		{
			cmpResult = key1.getSubject().compareTo(key2.getSubject());
			if(cmpResult == 0)
				cmpResult = key1.getObject().compareTo(key2.getObject());
		}
		return cmpResult;
}

}
