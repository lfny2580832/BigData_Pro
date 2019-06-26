package bigdata.mapreduce.log_analysis;

import bigdata.io.AdMetricWritable;
import bigdata.io.ComplexKeyWritable;
import bigdata.io.PVGroupComparator;
import bigdata.io.PVKeyWritable;
import bigdata.lib.PVPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SecondSortMRJobNew extends Configured implements Tool {

    //Map阶段
    public static class SecondSortMapper extends Mapper<LongWritable,Text, ComplexKeyWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            long pv = Long.valueOf(fields[2]);
            long click = Long.valueOf(fields[3]);
            ComplexKeyWritable complexKey = new ComplexKeyWritable(pv,click);
            String outputValue = fields[0] + "\t" + fields[1];
            context.write(complexKey,new Text(outputValue));
        }
    }

    //Reduce阶段
    public static class SecondSortReducer extends Reducer<ComplexKeyWritable, Text, Text, ComplexKeyWritable>{
        private int reduceCounter = 0;
        @Override
        protected void reduce(ComplexKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //使用自定义paritioner后，pv都为53的map数据交给一个reduce task来执行，不然没法实现复杂排序
            //使用comparator后，reduce方法就不用key的compareto来分组了。现在用pv来分组，pv一样的数据调用一个reduce方法

            //

            System.out.println("第" + reduceCounter + "次被调用！");
            for(Text value : values){
                //在这个循环里，key对应的是该value的key，这是框架
                System.out.println(key.toString());
                context.write(value,key);
            }
            reduceCounter += 1;
        }

    }

    @Override
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(SecondSortMRJobNew.class);

        //3.给job添加执行流程

        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);

        try {
            //job添加输入路径
            FileInputFormat.addInputPath(job,path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3.2设置map执行阶段
        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(ComplexKeyWritable.class);//map输出key类型
        job.setMapOutputValueClass(Text.class); //map输出value类型

        job.setPartitionerClass(PVPartitioner.class);//自定义Partitioner使用mapper输出key的一部分（pv）进行分区

        //3.3设置reduce执行阶段
        job.setReducerClass(SecondSortReducer.class);
        job.setOutputKeyClass(Text.class);//reduce输出key类型
        job.setOutputValueClass(ComplexKeyWritable.class);//reduce输出value类型

        //job.setNumReduceTasks(3);//硬编码，不灵活

        job.setGroupingComparatorClass(PVGroupComparator.class);//默认通过key的compareTo方法比较key，使用自定义的分组比较器

        //3.4设置job计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,output);

        //4. 提交job，并等待job执行完成

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) {
        //用于本地测试
        if(args.length == 0){
            args = new String[]{
                    "hdfs://ns/mr_project/output/MapSideJoinMRJob",
                    "hdfs://ns/mr_project/output/SecondSortJoinMRJob"
            };
        }
        //1.配置job
        Configuration conf = new Configuration();
        Path hdfsOutputPath = new Path(args[1]);//mr在hdfs上的输出路径
        try {
            //如果mr的输出结果路径存在，则删除
            FileSystem fileSystem = FileSystem.get(conf);
            if(fileSystem.exists(hdfsOutputPath)){
                fileSystem.delete(hdfsOutputPath,true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            int status = ToolRunner.run(conf,new SecondSortMRJobNew(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
