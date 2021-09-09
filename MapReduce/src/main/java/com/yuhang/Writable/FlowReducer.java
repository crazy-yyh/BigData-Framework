package com.yuhang.Writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yyh
 * @create 2021-04-28 14:39
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text,FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1 遍历集合累加值
        long totalUp = 0;
        long totaldown = 0;

        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totaldown += value.getDownFlow();
        }

        // 2 封装outk, outv
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totaldown);
        outV.setSum();

        // 3 写出
        context.write(key, outV);
    }
}
