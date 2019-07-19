import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * @author: hanj
 * @date: 2019/7/18
 * @description: 唯一键下的topN
 */
public class TopNDriver extends Configured implements Tool {
    private static Logger THE_LOGGER = Logger.getLogger(TopNDriver.class);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
//        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);
//        HadoopUtil.addJarsToDistributedCache(job, "/lib/");
//        int N = Integer.parseInt(args[0]);
//        job.getConfiguration().setInt("N",10);
        job.setJarByClass(TopNDriver.class);
        job.setJobName("TopNDriver");

//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            System.out.println("输出路径 " + new Path(args[1]).getName() + " 已存在,默认删除");
            fs.delete(new Path(args[1]), true);
        }
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        boolean status = job.waitForCompletion(true);
        System.out.println(status);
        THE_LOGGER.info("run():status="+status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[2];
        args[0]="G:\\data\\hadoop\\cat.txt";
        args[1]="G:\\data\\hadoop\\cat";
        int returnStatus = ToolRunner.run(new TopNDriver(),args);
        System.exit(returnStatus);
    }
}

class TopMapper extends Mapper<Object,Text,NullWritable,Text>{
    private int N = 2;
    private SortedMap<Double,Text> top10Cats = new TreeMap<>();
//    @Override
//    protected void setup(Context context){
//        this.N = context.getConfiguration().getInt("N",5);
//    }
    @Override
    public void map(Object key,Text values,Context context) throws IOException, InterruptedException {
        System.out.println("---------");
        String[] tokens = values.toString().split(",");
        Double catWeight = Double.parseDouble(tokens[0]);
        top10Cats.put(catWeight,values);
        //System.out.println(values.toString()+"========");
        if (top10Cats.size()>N){
            top10Cats.remove(top10Cats.firstKey());
        }
        for (Text catAttributes : top10Cats.values()){
            System.out.println(catAttributes+"===");
            context.write(NullWritable.get(),catAttributes);
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for (Text catAttributes : top10Cats.values()){
//           context.write(NullWritable.get(),catAttributes);
//        }
//    }
}

class TopReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
    private int N = 2;
    private SortedMap<Double,Text> finaleTop10 = new TreeMap<>();
    @Override
    protected  void setup(Context context){
        this.N = context.getConfiguration().getInt("N",10);
    }
    @Override
    public void reduce(NullWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for (Text catRecord:values){
            String[] tokens = catRecord.toString().split(",");
            Double weight = Double.parseDouble(tokens[0]);
            finaleTop10.put(weight,catRecord);
            if (finaleTop10.size()>N){
                finaleTop10.remove(finaleTop10.firstKey());
            }
        }
        for (Text text : finaleTop10.values()){
            context.write(NullWritable.get(),text);
        }
    }

}
