package tiger.sparkcore.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author Zenghu
 * @Date 2021/3/30 22:41
 * @Description
 * @Version: 1.0
 *
 * */
object PartitionTest {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
    config.setMaster("local[*]")
    config.setAppName("partition")

    val sc = new SparkContext(config)



    // 底层调用parallelize
    // 分区数：defaultParallelism
    //        SparkConf参数spark.default.parallelism： 默认值为CPU核数
     //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)


    // 分区数：
    /*

    var totalSize: Long = 0// 所有文件的大小
    for (file <- files)  { // check we have valid files
      if (file.isDirectory)  { throw new IOException("Not a file: " + file.getPath)
    }
     totalSize += file.getLen
    }

    val goalSize: Long = totalSize / (if (numSplits == 0)  { 1}else  { numSplits}) // numSplits=2
    // goalSize = 总的大小/分区数， 默认分区数为2

    val minSize: Long = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize)
   computeSplitSize = Math.max(minSize, Math.min(goalSize, blockSize));
     */

    val rdd: RDD[String] = sc.textFile("data/1.txt", 4)


    rdd.saveAsTextFile("out")
    sc.stop()
  }


}
