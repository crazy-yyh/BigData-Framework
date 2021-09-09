package com.yuhang.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yyh
 * @create 2021-04-28 14:59
 */
public class FlowBean implements Writable {

    private Long upFlow;
    private Long downFlow;
    private Long sum;

    public FlowBean() {

    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum() {
        this.sum = getUpFlow()+ getDownFlow();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sum);


    }

    @Override
    public void readFields(DataInput input) throws IOException {

        this.upFlow = input.readLong();
        this.downFlow = input.readLong();
        this.sum = input.readLong();

    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sum;
    }
}
