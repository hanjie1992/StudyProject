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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Objects;

/**
 * @author: hanj
 * @date: 2019/7/23
 * @description: 反转排序，计算单词相对频率
 */
public class RelativeFrequencyDriver extends Configured implements Tool {
    private static final Logger THE_LOGGER = Logger.getLogger(RelativeFrequencyDriver.class);

    public static void main(String[] args) throws Exception {
//        if (args.length != 3){
//            THE_LOGGER.warn("参数错误");
//            System.exit(-1);
//        }
        int status = ToolRunner.run(new RelativeFrequencyDriver(),args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        int neighborWindow = Integer.parseInt(args[0]);
//        Path inputpath = new Path(args[1]);
//        Path outputpath = new Path(args[2]);

        int neighborWindow = 1;
        Path inputpath = new Path("G:\\data\\hadoop\\RelativeFrequency.txt");
        Path outputpath = new Path("G:\\data\\hadoop\\RelativeFrequency");

        //如果文件存在就删除
        FileSystem.get(getConf()).delete(outputpath,true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(RelativeFrequencyDriver.class);
        job.setJobName("RelativeFrequencyDriver");

        job.getConfiguration().setInt("neighbor.window",neighborWindow);

        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);

        job.setMapOutputKeyClass(PairOfWords.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfWords.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setCombinerClass(RelativeFrequencyCombiner.class);
        job.setPartitionerClass(OrderInversionPartitioner.class);
        job.setNumReduceTasks(1);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        THE_LOGGER.info("job耗时"+(System.currentTimeMillis()-startTime));
        return 0;

    }
}

class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable>{
    @Override
    protected void reduce(PairOfWords key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int partialSum = 0;
        for (IntWritable value : values){
            partialSum += value.get();
        }
        context.write(key,new IntWritable(partialSum));
    }
}

class RelativeFrequencyMapper extends Mapper<LongWritable,Text,PairOfWords,IntWritable>{
    private int neighborWindow = 2;
    //pair = (leftElement,rightElement)
    private final PairOfWords pair = new PairOfWords();
    private final IntWritable totalCount = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void setup(Context context){
        this.neighborWindow = context.getConfiguration().getInt("neighbor.window",2);
    }

    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(),',');
        if ((tokens == null) || (tokens.length < 2)){
            return;
        }
        for (int i = 0; i < tokens.length;i++){
            tokens[i] = tokens[i].replaceAll("\\W+","");
            if (tokens[i].equals("")){
                continue;
            }
            System.out.println("token"+i+" = "+tokens[i]);
            pair.setWord(tokens[i]);

            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            System.out.println("start = "+start);
            int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;
            System.out.println("end = "+end);
            for (int j = start; j <= end; j++){
                if (j == i){
                    continue;
                }
                pair.setNeighbor(tokens[j].replaceAll("\\W",""));
                context.write(pair,ONE);
            }
            pair.setNeighbor("*");
            totalCount.set(end - start);
            context.write(pair,totalCount);
        }
    }
}

class RelativeFrequencyReducer extends Reducer<PairOfWords,IntWritable,PairOfWords,DoubleWritable>{
    private double totalCount = 0;
    private final DoubleWritable relativeCount = new DoubleWritable();
    private String currentWord = "NO_DEFINED";

    @Override
    protected void reduce(PairOfWords key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        if (key.getNeighbor().equals("*")){
            if (key.getWord().equals(currentWord)){
                totalCount += totalCount + getTotalCount(values);
            }else {
                currentWord = key.getWord();
                totalCount = getTotalCount(values);
            }
        }else {
            int count = getTotalCount(values);
            relativeCount.set((double) count/totalCount);
            context.write(key,relativeCount);
        }
    }
    private int getTotalCount(Iterable<IntWritable> values){
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        return sum;
    }
}

/**
 * 按照第一列相同分区leftWord
 */
class OrderInversionPartitioner extends Partitioner<PairOfWords,IntWritable>{

    @Override
    public int getPartition(PairOfWords key, IntWritable value, int numberOfPartitions) {

        String leftWord = key.getLeftElement();
        // key = (leftWord, rightWord) = (word, neighbor)
        return Math.abs( ((int)hash(leftWord) ) % numberOfPartitions);
    }

    //根据string.hashcode（）改编,返回给定字符串对象的哈希代码（）
    private static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31*h + str.charAt(i);
        }
        return h;
    }
}

/**
 * PairOfWords 代表一对字符串。这对元素被称为左（字）和右（邻）元素。自然排序顺序是：先按左元素排序，然后按右元素排序
 */
class PairOfWords implements WritableComparable<PairOfWords>{
    private String leftElement;
    private String rightElement;

    public PairOfWords() {
    }

    public PairOfWords(String left, String right) {
        set(left,right);
    }

    public void set(String left,String right){
        leftElement = left;
        rightElement = right;
    }

    public String getLeftElement() {
        return leftElement;
    }

    public void setLeftElement(String leftElement) {
        this.leftElement = leftElement;
    }

    public void setWord(String leftElement){
        setLeftElement(leftElement);
    }

    public String getWord(){
        return leftElement;
    }

    public String getRightElement() {
        return rightElement;
    }

    public void setRightElement(String rightElement) {
        this.rightElement = rightElement;
    }

    public void setNeighbor(String rightElement){
        setRightElement(rightElement);
    }

    public String getNeighbor(){
        return rightElement;
    }

    public String getKey(){
        return leftElement;
    }

    public String getValue(){
        return rightElement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairOfWords that = (PairOfWords) o;
        return Objects.equals(leftElement, that.leftElement) &&
                Objects.equals(rightElement, that.rightElement);
    }

    @Override
    public int hashCode() {

        return Objects.hash(leftElement, rightElement);
    }

    @Override
    public String toString() {
        return "PairOfWords{" +
                "leftElement='" + leftElement + '\'' +
                ", rightElement='" + rightElement + '\'' +
                '}';
    }

    /**
     * 定义成对的自然排序顺序。对首先按左元素排序，然后按右元素排序。
     */
    @Override
    public int compareTo(PairOfWords pair) {
        String p1 = pair.getLeftElement();
        String p2 = pair.getRightElement();

        if (leftElement.equals(p1)){
            return rightElement.compareTo(p2);
        }

        return leftElement.compareTo(p1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput,leftElement);
        Text.writeString(dataOutput,rightElement);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        leftElement = Text.readString(dataInput);
        rightElement = Text.readString(dataInput);
    }

    @Override
    public PairOfWords clone(){
        return new PairOfWords(this.leftElement,this.rightElement);
    }

    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(PairOfWords.class);
        }
        @Override
        public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
            try {
                int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
                int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
                int firstStrL1 = readVInt(b1, s1);
                int firstStrL2 = readVInt(b2, s2);
                int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1, b2, s2 + firstVIntL2, firstStrL2);
                if (cmp != 0) {
                    return cmp;
                }

                int secondVIntL1 = WritableUtils.decodeVIntSize(b1[s1 + firstVIntL1 + firstStrL1]);
                int secondVIntL2 = WritableUtils.decodeVIntSize(b2[s2 + firstVIntL2 + firstStrL2]);
                int secondStrL1 = readVInt(b1, s1 + firstVIntL1 + firstStrL1);
                int secondStrL2 = readVInt(b2, s2 + firstVIntL2 + firstStrL2);
                return compareBytes(b1, s1 + firstVIntL1 + firstStrL1 + secondVIntL1, secondStrL1, b2,
                        s2 + firstVIntL2 + firstStrL2 + secondVIntL2, secondStrL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(PairOfWords.class,new Comparator());
    }
}
