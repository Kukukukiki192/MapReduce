package com.k.mapred.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** reduce阶段输入输出类型
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     <Text, IntWritable, Text, IntWritable>
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //1.对同一个key的value 累加
        // k,(1,1)
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
//        IntWritable outV = new IntWritable(); //每次遍历都要创建新对象->往上提
        outV.set(sum);

        //2.输出
        context.write(key, outV);
    }
}