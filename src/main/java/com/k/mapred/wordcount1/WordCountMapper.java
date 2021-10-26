package com.k.mapred.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/** map阶段输入输出类型
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     <LongWritable, Text, Text, IntWritable>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);  //不进行聚合

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//        Text outK = new Text(); //每次map都要创建新对象->往上提

        //1.获取一行
        // k k
        String line = value.toString();

        //2.切割
        // k
        // k
        String[] words = line.split(" ");

        //3.循环输出
        for(String word: words) {
//            Text outK = new Text(); //每次遍历都要创建新对象->往上提
            //封装outK
            outK.set(word);
            //输出
            context.write(outK, outV);  // k,1
        }
    }
}