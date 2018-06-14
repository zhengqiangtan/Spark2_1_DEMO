package wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc wordcount示例
  * @Created by tzq 2018/6/14 15:36
  **/
object Test {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("test").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("file:///Users/tanzhengqiang/data/spark/wc.txt")

    rdd.flatMap(line => (line.split(" ")))
      .map(w => (w,1))
      .reduceByKey((x,y) => (x+y))
      .sortBy(_._2,false)
        .foreach(println(_))

    sc.stop()


  }

}
