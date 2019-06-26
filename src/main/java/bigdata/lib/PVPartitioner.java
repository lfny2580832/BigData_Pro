package bigdata.lib;

import bigdata.io.AdMetricWritable;
import bigdata.io.ComplexKeyWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class PVPartitioner extends Partitioner<ComplexKeyWritable,Text> {
    public int getPartition(ComplexKeyWritable key, Text value, int numReduceTasks) {
        return (int)key.getPv() % numReduceTasks ;
    }
}
