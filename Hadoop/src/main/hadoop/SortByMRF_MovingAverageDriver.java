import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import util.DateUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

/**
 * @author: hanj
 * @date: 2019/7/24
 * @description: 移动平均算法
 * 移动时间序列数据平均值的MapReduce作业
 */
public class SortByMRF_MovingAverageDriver {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf,SortByMRF_MovingAverageDriver.class);
        jobConf.setJobName("SortByMRF_MovingAverageDriver");
//        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
//        if (otherArgs.length != 3){
//            System.out.println("参数错误");
//            System.exit(1);
//        }
        Path inputpath = new Path("G:\\data\\hadoop\\SortByMRF_MovingAverage.txt");
        Path outputpath = new Path("G:\\data\\hadoop\\SortByMRF_MovingAverage");

        //如果文件存在就删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            System.out.println("输出路径 " + outputpath.getName() + " 已存在,默认删除");
            fs.delete(outputpath, true);
        }


        // add jars to distributed cache
//        HadoopUtil.addJarsToDistributedCache(conf,"/lib/");

        // set mapper/reducer
        jobConf.setMapperClass(SortByMRF_MovingAverageMapper.class);
        jobConf.setReducerClass(SortByMRF_MovingAverageReducer.class);

        //定义map输出格式
        jobConf.setMapOutputKeyClass(CompositeKey.class);
        jobConf.setMapOutputValueClass(TimeSeriesData.class);

        //定义reduce输出格式
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        //定义输入输出
        FileInputFormat.setInputPaths(jobConf,inputpath);
        FileOutputFormat.setOutputPath(jobConf,outputpath);
        //定义移动窗口的大小，移动平均算法的参数之一
//        int windowSize = Integer.parseInt(otherArgs[0]);
        int windowSize = 3;
        jobConf.setInt("moving.average.window.size",windowSize);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.setCompressMapOutput(true);//采用压缩减少输出的大小

        /**
         * “二级排序”分区程序需要以下3个设置来决定哪个映射器输出将根据映射器输出键转到哪个还原器。
         * 一般来说，不同的键在不同的组中（reducer端的迭代器）。但有时，我们希望同一组中有不同的键。
         * 这是输出值分组比较器的时间，用于对映射器输出进行分组（类似于SQL中的分组条件）。
         * 输出键比较器用于映射器输出键的排序阶段。
         */
        jobConf.setPartitionerClass(NaturalKeyPartitioner.class);
        jobConf.setOutputKeyComparatorClass(CompositeKeyComparator.class);
        jobConf.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);

        JobClient.runJob(jobConf);

    }
}

class SortByMRF_MovingAverageMapper extends MapReduceBase implements Mapper<LongWritable,Text,CompositeKey,TimeSeriesData> {
    private final CompositeKey reducerKey = new CompositeKey();
    private final TimeSeriesData reducerValue = new TimeSeriesData();

    @Override
    public void map(LongWritable inkey, Text value, OutputCollector<CompositeKey, TimeSeriesData> output, Reporter reporter) throws IOException {
        String record = value.toString();
        if ((record == null) || (record.length() == 0)) {
            return;
        }
        String[] tokens = StringUtils.split(record,',');
        if (tokens.length == 3) {
            // tokens[0] = name
            // tokens[1] = timestamp
            // tokens[2] = value of timeseries as double
            Date date = DateUtil.getDate(tokens[1]);
            if (date == null) {
                return;
            }
            long timestamp = date.getTime();
            System.out.println("======"+(tokens[0]+timestamp+tokens[2]+tokens[1]));
            reducerKey.set(tokens[0], timestamp);
            reducerValue.set(timestamp, tokens[2]);
            output.collect(reducerKey, reducerValue);
        } else {// log as error, not enough tokens}
        }
    }
}

class SortByMRF_MovingAverageReducer extends MapReduceBase implements Reducer<CompositeKey,TimeSeriesData,Text,Text>{
    int windowSize = 3;

    @Override
    public void configure(JobConf jobConf){
        this.windowSize = jobConf.getInt("moving.average.window.size",3);
    }

    @Override
    public void reduce(CompositeKey key, Iterator<TimeSeriesData> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text outputKey = new Text();
        Text outputValue = new Text();
        try {
            MovingAverage ma = new MovingAverage(this.windowSize);
            while (values.hasNext()){
                TimeSeriesData data = values.next();
                ma.addNewNumber(data.getValue());
                double movingAverage = ma.getMovingAverage();
                long timestamp = data.getTimestamp();
                String dateAsString = DateUtil.getDateAsString(timestamp);
                outputValue.set(dateAsString+","+movingAverage);
                outputKey.set(key.getName());
                output.collect(outputKey,outputValue);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}

/**
 * 实现了“移动平均”算法的基本功能。
 * 在Hadoop的shuffle阶段中使用，将组合键按其键的第一部分（natural）分组。时间序列数据的natural键是“name”
 */
class MovingAverage{

    private double sum = 0.0;
    private final int period;
    private double[] window = null;
    private int pointer = 0;
    private int size = 0;

    public MovingAverage(int period) throws IllegalAccessException {
        if (period<1){
            throw new IllegalAccessException("参数错误");
        }
        this.period = period;
        window = new double[period];
    }

    public void addNewNumber(String numbe){
        double number = Double.valueOf(numbe);
        sum += number;
        if (size < period){
            window[pointer++] = number;
            size++;
        }else {
            // size = period (size cannot be > period)
            pointer = pointer % period;
            sum -= window[pointer];
            window[pointer++] = number;
        }
    }
    public double getMovingAverage() throws IllegalAccessException {
        if (size == 0){
            throw new IllegalAccessException("移动平均值未定义");
        }
        return sum/size;
    }

}

/**
 * 按的第一部分（自然）对组合键进行分组,自然键是“名称”
 */
class NaturalKeyGroupingComparator extends WritableComparator{
    protected NaturalKeyGroupingComparator(){
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable w1,WritableComparable w2){
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        return key1.getName().compareTo(key2.getName());
    }
}

/**
 * 此类的目的是启用两个复合键的比较
 */
class CompositeKeyComparator extends WritableComparator{

    protected CompositeKeyComparator(){
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable w1,WritableComparable w2){
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

        int comparison = key1.getName().compareTo(key2.getName());
        if (comparison == 0){
            if (key1.getTimestamp() == key2.getTimestamp()){
                return 0;
            }else if (key1.getTimestamp() < key2.getTimestamp()){
                return -1;
            }else {
                return 1;
            }
        }else {
            return comparison;
        }
    }
}

/**
 * 由于我们希望单个reducer接收单个“name”的所有时间序列数据，
 * 因此我们只通过自然键组件（“name”）来划分映射阶段的数据输出。
 */
class NaturalKeyPartitioner implements Partitioner<CompositeKey,TimeSeriesData>{

    @Override
    public int getPartition(CompositeKey compositeKey, TimeSeriesData timeSeriesData, int numberOfPartitions) {
        return Math.abs((int)(hash(compositeKey.getName()) % numberOfPartitions));
    }

    @Override
    public void configure(JobConf jobConf) {
    }
    /**
     * 改编string.hashcode（）。
     */
    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}

/**
 * 自定义组合key,map输出的key
 *
 * compositekey：表示一对（字符串名称，长时间戳）
 *
 * 我们在name字段上进行一个主分组传递，以将一种类型的所有数据集合在一起，
 * 然后在shuffle阶段使用timestamp long字段对时间序列点进行排序，
 * 以使它们到达按排顺序划分的reducer。
 */
class CompositeKey implements WritableComparable<CompositeKey>{
    // 自然键 (name)
    // 组合键 (name, timestamp)
    private String name;
    private long timestamp;

    public CompositeKey() {
    }

    public CompositeKey(String name, long timestamp){
        set(name,timestamp);
    }

    public void set(String name, long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CompositeKey o) {
        if (this.name.compareTo(o.name) != 0){
            return this.name.compareTo(o.name);
        }else if (this.timestamp != o.timestamp){
            return timestamp < o.timestamp ? -1 : 1;
        }else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    public static class CompositeKeyComparator extends WritableComparator{
        public CompositeKeyComparator(){
            super(CompositeKey.class);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }
    static {
        WritableComparator.define(CompositeKey.class,new CompositeKeyComparator());
    }
}

/**
 * 自定义value,map输出的value
 * TimeSeriesData表示一对（时间序列时间戳、时间序列值）。
 */
class TimeSeriesData implements Writable,Comparable<TimeSeriesData>{

    private long timestamp;
    private String value;

    public TimeSeriesData() {
    }

    public static TimeSeriesData copy(TimeSeriesData tsd){
        return new TimeSeriesData(tsd.timestamp,tsd.value);
    }

    public TimeSeriesData(long timestamp, String value) {
        set(timestamp,value);
    }

    public void set(long timestamp, String value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getValue() {
        return this.value;
    }

    public String toString(){
        return "("+timestamp+","+value+")";
    }


    @Override
    public int compareTo(TimeSeriesData o) {
        if (this.timestamp < o.timestamp){
            return -1;
        }else if (this.timestamp > o.timestamp){
            return  1;
        }else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeUTF(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.value = dataInput.readLine();
    }
    //将二进制数据转换为时间序列数据
    public static TimeSeriesData read(DataInput in) throws IOException{
        TimeSeriesData tsData = new TimeSeriesData();
        tsData.readFields(in);
        return  tsData;
    }

    public String getDate(){
        return DateUtil.getDateAsString(this.timestamp);
    }

    public TimeSeriesData clone(){
        return new TimeSeriesData(timestamp,value);
    }
}


