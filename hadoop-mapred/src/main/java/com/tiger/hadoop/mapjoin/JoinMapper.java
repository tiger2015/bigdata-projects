package com.tiger.hadoop.mapjoin;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Zenghu
 * @Date 2021/2/27 17:13
 * @Description
 * @Version: 1.0
 **/
public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> products = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        URI productUri = null;
        for (URI cacheFile : cacheFiles) {
            if (cacheFile.getPath().contains("product.txt")) {
                productUri = cacheFile;
                break;
            }
        }
        if (productUri != null) {
            try (FSDataInputStream inputStream = FileSystem.get(productUri, context.getConfiguration()).open(new Path("/data/join/input/product.txt"));
                 BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] split = line.split(",");
                    products.put(split[0], line);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String productId = split[1];
        String s = products.get(productId);
        if (s != null) {
            context.write(new Text(value + "," + s), NullWritable.get());
        }
    }
}
