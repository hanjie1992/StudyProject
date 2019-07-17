import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author: hanj
 * @date: 2019/7/17
 * @description: 二次排序
 */
public class SecondarySortDriver extends Configured implements Tool {
    private static Logger theLogger = Logger.getLogger(SecondarySortDriver.class);
    public static void main(String[] args) throws Exception {
        args = new String[2];
        args[0]="G:\\data\\hadoop\\temperature.txt";
        args[1]="G:\\data\\hadoop\\temperature";

        if (args.length != 2) {
            theLogger.warn("SecondarySortDriver <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
        }

        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        theLogger.info("returnStatus="+returnStatus);
        System.out.println(returnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySortDriver");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            System.out.println("输出路径 " + new Path(args[1]).getName() + " 已存在,默认删除");
            fs.delete(new Path(args[1]), true);
        }

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortingMapper.class);
        job.setReducerClass(SecondarySortReduce.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        theLogger.info("run():status="+status);
        return status ? 0 : 1;
    }
}

class SecondarySortingMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text>{
    private final Text theTemperature = new Text();
    private final DateTemperaturePair pair = new DateTemperaturePair();
    @Override
    protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
        String line = value.toString();
        String[] tokens = line.split(",");
        String yearMonth = tokens[0]+tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);
        pair.setYearMonth(yearMonth);
        pair.setDay(day);
        pair.setTemperature(temperature);
        theTemperature.set(tokens[3]);
        context.write(pair,theTemperature);
    }
}

class SecondarySortReduce extends Reducer<DateTemperaturePair,Text,Text,Text>{
    protected void reduce(DateTemperaturePair key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value:values){
            builder.append(value.toString());
            builder.append(",");
        }
        context.write(key.getYearMonth(),new Text(builder.toString()));
    }
}

/**
 * 定制分区器
 * */
class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair,Text>{
    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int numberOfPartitions) {
        return Math.abs(pair.getYearMonth().hashCode()% numberOfPartitions);
    }
}

/**
 * 分组比较器
 * */
class DateTemperatureGroupingComparator extends WritableComparator{
    public DateTemperatureGroupingComparator(){
       super(DateTemperaturePair.class,true);
    }
    @Override
    public int compare(WritableComparable wc1,WritableComparable wc2){
        DateTemperaturePair pair = (DateTemperaturePair)wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair)wc2;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
/**
 * 排序
 * */
class DateTemperaturePair implements Writable,WritableComparable<DateTemperaturePair>{

    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();

    DateTemperaturePair() {
    }
    @Override
    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue==0){
            compareValue = temperature.compareTo(pair.getTemperature());
        }
//        return compareValue;//升序
        return -1*compareValue;//降序
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        yearMonth.write(dataOutput);
        day.write(dataOutput);
        temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth.readFields(dataInput);
        day.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public IntWritable getTemperature() {
        return temperature;
    }
    public void setYearMonth(String yearMonthAsString){
        yearMonth.set(yearMonthAsString);
    }
    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTemperaturePair pair = (DateTemperaturePair) o;
        return Objects.equals(yearMonth, pair.yearMonth) &&
                Objects.equals(day, pair.day) &&
                Objects.equals(temperature, pair.temperature);
    }

    @Override
    public int hashCode() {

        return Objects.hash(yearMonth, day, temperature);
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DateTemperaturePair{yearMonth=");
        builder.append(yearMonth);
        builder.append(", day=");
        builder.append(day);
        builder.append(", temperature=");
        builder.append(temperature);
        builder.append("}");
        return builder.toString();
    }
}
