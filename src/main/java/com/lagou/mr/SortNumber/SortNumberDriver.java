package com.lagou.mr.SortNumber;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 驱动类
 */
public class SortNumberDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取配置文件对象
        Configuration conf = new Configuration();

        //2.判断参数是否符合情况
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
        {
            System.err.println("Usage: SortNumberDriver <in> [<in>...] <out>");
            System.exit(2);
        }


        //3.获取job对象实例
        Job job = Job.getInstance(conf, "SortNumberDriver");

        //4.指定程序jar的本地路径
        job.setJarByClass(SortNumberDriver.class);
        //5.指定程序jar的Mapper/Reduce类
        job.setMapperClass(SortNumberMapper.class);
        job.setReducerClass(SortNumberReduce.class);

        //6.指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(SortNumberBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //7. 指定最终输出的kv数据类型
        job.setOutputKeyClass(SortNumberBean.class);
        job.setOutputValueClass(NullWritable.class);

        //8.文件小，使用CombineTextInputFormat就可以了
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置4kb 
        CombineTextInputFormat.setMaxInputSplitSize(job, 4096);

        //9.指定文件的输入路径，考虑多个文件的输入路径
        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

        //10.提交作业,jvm退出：正常退出0，非0值则是错误退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
