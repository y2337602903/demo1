package com.lagou.mr.SortNumber;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortNumberMapper extends Mapper<LongWritable, Text, SortNumberBean, NullWritable> {

    /**
     * 用Bean对象给每个数据添加序号，方便输出
     * SortNumerBean实现WritableComparable接口按照数字排序
     */
    SortNumberBean sortNumerBean = new SortNumberBean();



    /**
     * Map阶段，把每行数据放到Bean对象里面，排序以及添加序号
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //简单的处理一下字符串
        if(key.toString()!=null&&key.toString().length()>0&&key.toString().trim().length()>0){
            //需要预先设置一个序号，不然会报NPE
            sortNumerBean.setId(0L);
            sortNumerBean.setNumber(Long.parseLong(value.toString().trim()));
            context.write(sortNumerBean,NullWritable.get());
        }
    }
}
