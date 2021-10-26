package com.k.mapred.wordcount1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 提交到集群
 */
public class WordCountDriver {
    //打包到集群就不用加载本地文件了
//    //让程序强制加载bin目录下的hadoop.dll->防止报错 UnsatisfiedLinkError
//    static {
//        try {
//            System.load("E:/Hadoop/hadoop-3.1.3/bin/hadoop.dll");
//        } catch (UnsatisfiedLinkError e) {
//            System.err.println("Native code library failed to load.\n" + e);
//            System.exit(1);
//        }
//    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1.获取job
        Configuration conf = new Configuration();   //创建配置文件
        Job job = Job.getInstance(conf);

        // 2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3.关联mapper reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终输出的kv类型 (reduce输出的不是最终输出)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6.设置输入输出路径                 动态参数传入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
