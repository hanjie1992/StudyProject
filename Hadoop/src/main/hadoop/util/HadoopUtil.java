package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//

/**
 * @author: hanj
 * @date: 2019/7/24
 * @description:
 */
public class HadoopUtil {
    /**
     * 将所有JAR文件添加到HDFS的分布式缓存中
     * @param job job which will be run
     * @param hdfsJarDirectory a directory which has all required jar files
     */
    public static void addJarsToDistributedCache(Job job, String hdfsJarDirectory) throws IOException {
        if (job == null) {
            return;
        }
        addJarsToDistributedCache(job.getConfiguration(), hdfsJarDirectory);
    }

    /**
     * 将所有JAR文件添加到HDFS的分布式缓存中
     * @param conf conf which will be run
     * @param hdfsJarDirectory a directory which has all required jar files
     */
    public static void addJarsToDistributedCache(Configuration conf, String hdfsJarDirectory) throws IOException {
        if (conf == null) {
            return;
        }
        FileSystem fs = FileSystem.get(conf);
        List<FileStatus> jars = getDirectoryListing(hdfsJarDirectory, fs);
        for (FileStatus jar : jars) {
            Path jarPath = jar.getPath();
            DistributedCache.addFileToClassPath(jarPath, conf, fs);
        }
    }


    /**
     * 从给定的hdfs目录获取文件列表
     * @param directory an HDFS directory name
     * @param fs an HDFS FileSystem
     */
    public static List<FileStatus> getDirectoryListing(String directory, FileSystem fs) throws IOException {
        Path dir = new Path(directory);
        FileStatus[] fstatus = fs.listStatus(dir);
        return Arrays.asList(fstatus);
    }

    public static List<String> listDirectoryAsListOfString(String directory, FileSystem fs) throws IOException {
        Path path = new Path(directory);
        FileStatus fstatus[] = fs.listStatus(path);
        List<String> listing = new ArrayList<String>();
        for (FileStatus f: fstatus) {
            listing.add(f.getPath().toUri().getPath());
        }
        return listing;
    }


    /**
     * 如果存在hdfs路径，则返回true；否则返回false.
     */
    public static boolean pathExists(Path path, FileSystem fs)  {
        if (path == null) {
            return false;
        }

        try {
            return fs.exists(path);
        }
        catch(Exception e) {
            return false;
        }
    }
}
