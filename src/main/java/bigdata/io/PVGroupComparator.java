package bigdata.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PVGroupComparator extends WritableComparator{
    public PVGroupComparator(){
        super(ComplexKeyWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComplexKeyWritable key1 = (ComplexKeyWritable) a;
        ComplexKeyWritable key2 = (ComplexKeyWritable) b;
        return key1.getPv() == key2.getPv() ? 0 : 1;
    }
}
