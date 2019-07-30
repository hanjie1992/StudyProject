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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @author: hanj
 * @date: 2019/7/29
 * @description: 求共同好友
 * 数据：逗号前是用户id，后面为该用户对应的好友列表
 * <ul>
 * <li> 1,2 3 4 5 6</li>
 * <l1>2,1 3 4</li>
 * <li>3,1 2 4 5</li>
 * <ul>
 */
public class CommonFriendsDriver extends Configured implements Tool {
    private static final Logger theLogger = Logger.getLogger(CommonFriendsDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("CommonFriendsDriver");
        job.setJarByClass(CommonFriendsDriver.class);

//        HadoopUtil.addJarsToDistributedCache(job,"/lib/");

        String input = "G:\\data\\hadoop\\CommonFriends.txt";
        String output = "G:\\data\\hadoop\\CommonFriends";

        Path inputPath = new Path(input);
        Path outputpath = new Path(output);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //如果文件存在就删除
        FileSystem.get(getConf()).delete(outputpath,true);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CommonFriendsMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputpath);

        boolean status = job.waitForCompletion(true);
        theLogger.info("run():status= "+status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
//        if (args.length != 2){
//            throw new IllegalArgumentException("参数错误");
//        }
        int status = ToolRunner.run(new CommonFriendsDriver(),args);
        theLogger.info("status= "+status);
        System.exit(status);
    }
}

class CommonFriendsMapper extends Mapper<LongWritable,Text,Text,Text>{
    private static final Text REDUCE_KEY = new Text();
    private static final Text REDUCE_VALUE = new Text();

    static String getFriends(String[] tokens){
//        if (tokens.length == 2){
//            return "";
//        }
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i<tokens.length; i++){
            builder.append(tokens[i]);
            if (i < (tokens.length - 1)){
                builder.append(",");
            }
        }
        return builder.toString();
    }

    static String buildSortedKey(String person,String friend){
        long p = Long.parseLong(person);
        long f = Long.parseLong(friend);
        if (p<f){
            return person+","+friend;
        }else {
            return friend+","+person;
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(),',');
//        System.out.println("tokens= "+Arrays.toString(tokens));
        String fridens = getFriends(tokens);
//        System.out.println("friend= "+fridens);
        REDUCE_VALUE.set(fridens);
        String person = tokens[0];
        for (int i=1;i<tokens.length;i++){
            String friend = tokens[i];
            String reducerKeyAsString = buildSortedKey(person,friend);
            REDUCE_KEY.set(reducerKeyAsString);
            context.write(REDUCE_KEY,REDUCE_VALUE);
        }
    }
}

class CommonFriendsReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        System.out.println("key= "+key.toString());
        Map<String,Integer> map = new HashMap<String, Integer>();
        Iterator<Text> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()){
            String friends = iterator.next().toString();
            System.out.println("friends = "+friends);
            if (friends.equals("")){
                context.write(key,new Text("[]"));
            }
            addFriends(map,friends);
            numOfValues++;
        }
        List<String> commonFriends = new ArrayList<>();
        for (Map.Entry<String,Integer> entry: map.entrySet()){
            if (entry.getValue() == numOfValues){
                commonFriends.add(entry.getKey());
            }
        }
        context.write(key,new Text(commonFriends.toString()));
    }

    static void addFriends(Map<String, Integer> map, String friendsList) {
        String[] friends = StringUtils.split(friendsList,',');
        for (String friend : friends){
            Integer count = map.get(friend);
            if (count == null){
                map.put(friend,1);
            }else {
                map.put(friend,++count);
            }
        }
    }

}




