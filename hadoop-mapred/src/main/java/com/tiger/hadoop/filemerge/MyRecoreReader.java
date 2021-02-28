package com.tiger.hadoop.filemerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 16:38
 * @Description
 * @Version: 1.0
 **/
public class MyRecoreReader extends RecordReader<NullWritable, BytesWritable> {
    private FSDataInputStream fsDataInputStream = null; // 文件输入流
    private int length = 0; // 分片长度
    private int pos = 0; // 当前读取的位置
    private BytesWritable value = null;

    /**
     * 初始化
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        // 注意：configurarion中需要配置HDFS参数：fs.defaultFS，默认是读取本地
        FileSystem fileSystem = FileSystem.get(configuration);
        fsDataInputStream = fileSystem.open(path);
        length = (int) fileSplit.getLength();
    }

    /**
     * 读取Key,Value
     *
     * @return true: 读取到键值对
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (pos == length) {
            return false;
        }
        value = new BytesWritable();
        byte[] buffer = new byte[length];
        fsDataInputStream.read(buffer, 0, length);
        pos += length;
        value.set(buffer, 0, length);
        return true;
    }


    /**
     * 获取key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 关闭操作
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (fsDataInputStream != null) {
            IOUtils.closeStream(fsDataInputStream);
        }
    }
}
