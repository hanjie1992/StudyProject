import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import util.Combination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: hanj
 * @date: 2019/7/26
 * @description: 购物篮分析
 *
 * 数据：
 * <ul>
 * <li>顾客1:牛排, 玉米, 饼干, 可乐, 蜂蜜 </li>
 * <l1>顾客2:冰淇淋, 苹果, 面包, 肥皂</li>
 * <li>...</li>
 * <ul>
 * key: 第一个参数(:前)
 * value: :后的参数
 */
public class MBADriver extends Configured implements Tool{
    public static final Logger THE_LOGGER = Logger.getLogger(MBADriver.class);

    public static void main(String[] args) throws Exception {
//        if (args.length != 3){
//            printUsage();
//            System.exit(1);
//        }
        int exitStatus = ToolRunner.run(new MBADriver(),args);
        THE_LOGGER.info("exitStatus= "+exitStatus);
        System.exit(exitStatus);
    }

    private static int printUsage(){
        System.out.println("参数错误");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    @Override
    public int run(String args[]) throws Exception {
//        String inputPath = args[0];
//        String outputPath = args[1];
//        int numberOfPairs = Integer.parseInt(args[2]);

        String inputPath = "G:\\data\\spark\\MBADriver.txt";
        String outputPath = "G:\\data\\spark\\MBADriver";
        int numberOfPairs = 2;

        THE_LOGGER.info("inputPath: " + inputPath);
        THE_LOGGER.info("outputPath: " + outputPath);
        THE_LOGGER.info("numberOfPairs: " + numberOfPairs);

        Job job = Job.getInstance();
        job.setJobName("");
        job.getConfiguration().setInt("number.of.pairs",numberOfPairs);
        job.setJarByClass(MBADriver.class);

//        HadoopUtil.addJarsToDistributedCache(job,"/lib/");
        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MBAMapper.class);
        job.setCombinerClass(MBAReducer.class);
        job.setReducerClass(MBAReducer.class);

        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir,true);

        long startTime = System.currentTimeMillis();
        boolean staus = job.waitForCompletion(true);
        THE_LOGGER.info("Job status="+ staus);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        THE_LOGGER.info("Elapsed time:"+ elapsedTime + "毫秒");
        return staus ? 0 : 1;
    }
}

class MBAMapper extends Mapper<LongWritable, Text , Text ,IntWritable>{
    public static final Logger THE_LOGGER = Logger.getLogger(MBAMapper.class);

    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;

    private static final Text reducerKey = new Text();//key

    private static final IntWritable NUMBER_ONE = new IntWritable(1);//value

    int numberOfPairs;

    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs",DEFAULT_NUMBER_OF_PAIRS);
        THE_LOGGER.info("setup() numberOfPairs = "+numberOfPairs);
    }

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<String> items = convertItemsToList(line);
        if (items == null || (items.isEmpty())){
            return;
        }
        generrateMapperOutput(numberOfPairs,items,context);
    }
    private static List<String> convertItemsToList(String line) {
        if (line == null || (line.length() == 0)){
            return null;
        }
        String[] tokens = StringUtils.split(line,',');
        if (tokens == null || (tokens.length == 0 )){
            return null;
        }
        List<String> items = new ArrayList<String>();
        for (String token : tokens){
            if (token != null){
                items.add(token.trim());
            }
        }
        return items;
    }
    /**
     * 如果不对输入进行排序，它可能有重复的列表，但不被视为相同的列表。
     * @param numberOfPairs 关联的对数
     * @param items 输入的内容
     * @param context
     */
    private void generrateMapperOutput(int numberOfPairs, List<String> items, Context context) throws IOException, InterruptedException {
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(items,numberOfPairs);
        for (List<String> itemList : sortedCombinations){
            System.out.println("itemList=" + itemList.toString());
            reducerKey.set(itemList.toString());
            context.write(reducerKey,NUMBER_ONE);
        }
    }
}

class MBAReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        System.out.println("key="+key+" sum="+sum);
        context.write(key,new IntWritable(sum));
    }
}


















