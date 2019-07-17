import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: hanj
 * @date: 2019/7/3
 * @description:
 */
public class PhoneOdData {

    static class ExpandMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String id = line[0];
            String startTime = line[1];
            String startAddress = line[2];
            context.write(new Text(startAddress),one);
        }
    }

    static class ExpandReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val:values){
                sum += val.get();
            }
            IntWritable total = new IntWritable(sum);
            context.write(key,total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String in = "G:\\data\\od_20181210.txt";
        String out = "G:\\data\\out\\od_20181210";
        Job job = Job.getInstance(configuration);
        job.setJobName("PhoneOdData");
        job.setJarByClass(PhoneOdData.class);
        job.setMapperClass(ExpandMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ExpandReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        Path inPath = new Path(in);
        Path outPath = new Path(out);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outPath)) {
            System.out.println("输出路径 " + outPath.getName() + " 已存在,默认删除");
            fs.delete(outPath, true);
        }
        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
       // job.waitForCompletion(true);
       System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
