import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Iterator;

/**
 * @author: hanj
 * @date: 2019/7/19
 * @description: 使用MR实现左外连接关联查询
 */
class LeftJoinDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LeftJoinDriver.class);
        job.setJobName("LeftJonDriver");

        Path transactions = new Path("G:\\data\\hadoop\\transactions.txt");
        Path users = new Path("G:\\data\\hadoop\\users.txt");
        Path output = new Path("G:\\data\\hadoop\\LeftJoinDriver");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            System.out.println("输出路径 " + output.getName() + " 已存在,默认删除");
            fs.delete(output, true);
        }

        //“辅助排序”通过设置以下3个插件来处理：
        //1.映射器生成的键将如何分区
        job.setPartitionerClass(SecondarySortPartitioner.class);
        //2.自然键（由映射器生成）如何分组
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
        //3.如何对pairofstrings进行排序
//        job.setSortComparatorClass(PairOfStrings.class);

        job.setReducerClass(LeftJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setOutputFormatClass(Text.class);

        MultipleInputs.addInputPath(job,transactions, TextInputFormat.class,LeftJoinTransactionMapper.class);
        MultipleInputs.addInputPath(job,users,TextInputFormat.class,LeftJoinUserMapper.class);

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(PairOfStrings.class);
        FileOutputFormat.setOutputPath(job,output);
        if (job.waitForCompletion(true)){
            return;
        }
        else {
            throw new Exception("程序错误");
        }
    }
}

class PairOfStrings implements Writable, WritableComparable<PairOfStrings>{
    private final Text userId = new Text();
    private final Text locationId = new Text();

    public PairOfStrings() {
    }


    public Text getUserId() {
        return userId;
    }

    public Text getLocationId() {
        return locationId;
    }

    public void setUserId(String userIdString) {
        userId.set(userIdString);
    }
    public void setLocationId(String locationIdString) {
        locationId.set(locationIdString);
    }


    @Override
    public int compareTo(PairOfStrings pair) {
        int compareValue = this.userId.compareTo(pair.getUserId());
        if (compareValue == 0){
            compareValue = locationId.compareTo(pair.getLocationId());
        }
        return compareValue;
//        return -1*compareValue; //降序
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        locationId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        locationId.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairOfStrings that = (PairOfStrings) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(locationId, that.locationId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(userId, locationId);
    }

    @Override
    public String toString() {
        return "PairOfStrings{" +
                "userId=" + userId +
                ", locationId=" + locationId +
                '}';
    }
}

class LeftJoinUserMapper extends Mapper<LongWritable,Text,PairOfStrings,PairOfStrings>{
    PairOfStrings outputkey = new PairOfStrings();
    PairOfStrings outputvalue = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), ',');
        if (tokens.length == 2){
            outputkey.setUserId(tokens[0]);
            outputkey.setLocationId(tokens[1]);
            outputvalue.setUserId(tokens[0]);
            outputvalue.setLocationId(tokens[1]);
//            outputkey = new PairOfStrings(new Text(tokens[0]),new Text("1"));
//            outputvalue= new PairOfStrings(new Text("L"),new Text(tokens[1]));
            context.write(outputkey,outputvalue);
        }
    }
}
/**
 * Left Join”设计模式的事务部分实现map（）函数。
 */
class LeftJoinTransactionMapper extends Mapper<LongWritable,Text,PairOfStrings,PairOfStrings>{
    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(),',');
        String productID = tokens[1];
        String userID = tokens[2];
        outputKey.setUserId(userID);
        outputKey.setLocationId(productID);
        outputValue.setUserId(userID);
        outputValue.setLocationId(productID);
        context.write(outputKey,outputValue);
    }
}

class LeftJoinReducer extends Reducer<PairOfStrings,PairOfStrings,Text,Text>{
    Text productID = new Text();
    Text localtionID = new Text();

    @Override
    public void reduce(PairOfStrings key,Iterable<PairOfStrings> values,Context context) throws IOException, InterruptedException {
        Iterator<PairOfStrings> iterator = values.iterator();
        if (iterator.hasNext()){
            PairOfStrings firstPair = iterator.next();
            System.out.println("firstPair= "+firstPair.toString());
            if (firstPair.getUserId().equals(firstPair.getUserId())){
                localtionID.set(firstPair.getLocationId());
            }
        }
        while (iterator.hasNext()){
            PairOfStrings productPair = iterator.next();
            System.out.println("productPair="+productPair.toString());
            productID.set(productPair.getLocationId());
        }
        context.write(productID,localtionID);
    }
}

/**
 * 指示如何比较用户ID。
 * */
class SecondarySortGroupComparator implements RawComparator<PairOfStrings>{
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        DataInputBuffer buffer = new DataInputBuffer();
        PairOfStrings a = new PairOfStrings();
        PairOfStrings b = new PairOfStrings();
        try {
            buffer.reset(b1,s1,l1);
            a.readFields(buffer);
            buffer.reset(b2,s2,l2);
            b.readFields(buffer);
            return compare(a,b);
        }
        catch (Exception e){
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public int compare(PairOfStrings first, PairOfStrings second) {
        return first.getUserId().compareTo(second.getUserId());
    }
}

/**
 * 对数据进行分区。
 * */
class SecondarySortPartitioner extends Partitioner<PairOfStrings,Objects>{
    @Override
    public int getPartition(PairOfStrings key, Objects value, int numberOfPartitions) {
        return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
    }
}


