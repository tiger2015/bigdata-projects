package com.tiger.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author Zenghu
 * @Date 2021/2/18 0:19
 * @Description
 * @Version: 1.0
 **/
public class HDFSUtil {
    private final static String URL = "hdfs://dev:9000";
    private final static String USER = "tiger";
    private static Configuration configuration;


    static {
        // System.setProperty("HADOOP_USER_NAME", "tiger");
        configuration = new Configuration();
        // configuration.set("fs.defaultFS", "hdfs://tiger:9000");
        //configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }


    public static boolean mkdir(String dir) throws URISyntaxException, IOException, InterruptedException {
        try (FileSystem fileSystem = FileSystem.get(new URI(URL), configuration, USER)) {
            return fileSystem.mkdirs(new Path(dir));
        }
    }


    public static boolean exists(String path) throws IOException, URISyntaxException, InterruptedException {
        try (FileSystem fileSystem = FileSystem.get(new URI(URL), configuration, USER)) {
            return fileSystem.exists(new Path(path));
        }
    }


    public static boolean delete(String path, boolean recursive) throws IOException, URISyntaxException, InterruptedException {
        try (FileSystem fileSystem = FileSystem.get(new URI(URL), configuration, USER)) {
            return fileSystem.delete(new Path(path), recursive);
        }
    }


    public static void upload(String source, String dest) throws IOException, URISyntaxException, InterruptedException {
        try (FileSystem fileSystem = FileSystem.get(new URI(URL), configuration, USER)) {
            fileSystem.copyFromLocalFile(false, new Path(source), new Path(dest));
        }
    }

    public static void download(String source, String dest) throws IOException, URISyntaxException, InterruptedException {
        try (FileSystem fileSystem = FileSystem.get(new URI(URL), configuration, USER)) {
            fileSystem.copyToLocalFile(false, new Path(source), new Path(dest), true);
        }
    }

    public static List<FileStatus> list(String source, boolean recursive) throws URISyntaxException, IOException, InterruptedException {
        List<FileStatus> fileStatuses = new ArrayList<>();
        try (FileSystem fileSystem = FileSystem.get(new URI(URL), configuration, USER)) {
            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(new Path(source), recursive);
            while (fileStatusRemoteIterator.hasNext()) {
                fileStatuses.add(fileStatusRemoteIterator.next());
            }
        }
        return fileStatuses;
    }
}
