package com.yuhang.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yyh
 * @create 2021-04-28 9:13
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outK = new Text();

    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] s = line.split(" ");

        for (String s1 : s) {

            outK.set(s1);

            context.write(outK,outV);
        }

    }
}
