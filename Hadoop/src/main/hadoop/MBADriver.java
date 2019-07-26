import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

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
        if (args.length != 3){
            printUsage();
            System.exit(1);
        }

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
        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfPairs = Integer.parseInt(args[2]);

        THE_LOGGER.info("inputPath: " + inputPath);
        THE_LOGGER.info("outputPath: " + outputPath);
        THE_LOGGER.info("numberOfPairs: " + numberOfPairs);

        Job job = Job.getInstance();
        job.setJobName("");
        job.getConfiguration().setInt("number.of.pairs",numberOfPairs);

//        HadoopUtil.addJarsToDistributedCache(job,"/lib/");
        FileInputFormat.setInputPaths(job,new Path(inputPath));

        return 0;
    }
}
