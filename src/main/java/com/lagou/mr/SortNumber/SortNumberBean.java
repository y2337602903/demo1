package com.lagou.mr.SortNumber;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortNumberBean implements WritableComparable<SortNumberBean> {


    @Override
    public int compareTo(SortNumberBean o) {
        return (this.number < o.number) ? -1 : ((this.number.longValue() == o.number.longValue()) ? 0 : 1);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(number);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.number = in.readLong();
    }

    public SortNumberBean() {
    }

    public SortNumberBean(Long id, Long number) {
        this.id = id;
        this.number = number;
    }

    /**
     * 排序序号
     */
    private Long id;
    /**
     * 数字
     */
    private Long number;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }

    @Override
    public String toString() {
        return id + "\t" + number;
    }

}
