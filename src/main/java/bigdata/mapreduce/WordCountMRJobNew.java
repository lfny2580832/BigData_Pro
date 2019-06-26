package bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountMRJobNew extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //1.配置job
        Configuration conf = this.getConf();

        //2.创建job
        Job job = null;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(WordCountMRJob.class);

        //3.给job添加执行流程

        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[1]);
        try {
            FileInputFormat.addInputPath(job, path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3.2 设置map执行阶段
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3.3 设置reduce执行阶段
        job.setReducerClass(WordCountMapper.WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //3.4 设置job计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //4 提交job，并等待job执行完成
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

        return 0;
    }
    //map
    //输入数据的偏移量、输入数据类型、输出数据key的类型、输出数据value的类型

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] words = line.split("\t");

            for (String word : words) {
                //word 1
                context.write(new Text(word), new IntWritable(1));
            }

        }

        //前两个输入key、value数据类型，后两个输出key、value数据类型
        public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                //word {1,1,1,1...}

                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                context.write(key, new IntWritable(sum));
            }
        }

        public static void main(String[] args) {

            //用于本地测试
            if (args.length == 0) {
                args = new String[]{
                        "hdfs://ns/input/wc/",
                        "hdfs://ns/output/wc"
                };
            }
            Configuration conf = new Configuration();
            Path hdfsOutputPath = new Path(args[1]);    //mr在hdfs上的输出路径
            try {
                FileSystem fileSystem = null;
                fileSystem = FileSystem.get(conf);
                if (fileSystem.exists(hdfsOutputPath)) {
                    fileSystem.delete(hdfsOutputPath, true);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                int status = ToolRunner.run(conf,new WordCountMRJobNew(),args);
                System.exit(status);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    //reduce


}
