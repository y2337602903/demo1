package com.lagou.mr.SortNumber;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortNumberReduce extends Reducer<SortNumberBean, NullWritable, SortNumberBean, NullWritable> {

    /**
     * 创建一个类变量，来控制序号
     */
    static Long id = 0L;


    /**
     * recude阶段时给每个数字添加正确的序号
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(SortNumberBean key, Iterable<NullWritable> values, Context context) {
        //因为存在相同的数字，不能过滤掉，也要排序，而compareTo方法会导致两个对象合并key，所以需要遍历values获取每个key（bean对象）
        //遍历value同时，key也会随着遍历。
        values.forEach(value -> {
            id++;
            key.setId(id);
            try {
                context.write(key, value);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

    }
}
