import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length != 3){
            System.out.println("参数错误");
            System.exit(1);
        }

        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache(conf,"/lib/");

        // set mapper/reducer
        jobConf.setMapperClass();
        jobConf.setReducerClass();

        //定义map输出格式
        jobConf.setMapOutputKeyClass();
        jobConf.setMapOutputValueClass();

        //定义reduce输出格式
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);

        //定义移动窗口的大小，移动平均算法的参数之一
        int windowSize = Integer.parseInt(otherArgs[0]);

        jobConf.setInt("moving.average.window.size",windowSize);

        //定义输入输出
        FileInputFormat.setInputPaths(jobConf,new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobConf,new Path(otherArgs[2]));

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.setCompressMapOutput(true);//采用压缩减少输出的大小

        /**
         * “二级排序”分区程序需要以下3个设置来决定哪个映射器输出将根据映射器输出键转到哪个还原器。
         * 一般来说，不同的键在不同的组中（reducer端的迭代器）。但有时，我们希望同一组中有不同的键。
         * 这是输出值分组比较器的时间，用于对映射器输出进行分组（类似于SQL中的分组条件）。
         * 输出键比较器用于映射器输出键的排序阶段。
         */
        jobConf.setPartitionerClass();
        jobConf.setOutputKeyComparatorClass();
        jobConf.setOutputValueGroupingComparator();

        JobClient.runJob(jobConf);

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
class TimeSeriesData implements Writable,com{

}
