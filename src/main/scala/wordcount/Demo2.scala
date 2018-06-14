package wordcount

import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

/**
  * @Desc 统计文件请求数
  *
  *通过分析CDN或者Nginx的日志文件，统计出访问的PV、UV、IP地址、访问来源等相关数据
  *
  * 数据格式：
  * IP 命中率 响应时间 请求时间 请求方法 请求URL    请求协议 状态吗 响应大小 referer 用户代理
  * 223.93.159.226 HIT 203 [15/Feb/2017:11:14:35 +0800] "GET http://v-cdn.abc.com.cn/141035.mp4 HTTP/1.1" 206 5444007 "http://www.abc.com.cn/" "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko Core/1.53.2141.400 QQBrowser/9.5.10219.400"
  *
  * 需求1：统计访问次数最多的前10个IP <ip1,1233>
  *
  * 需求2：根据Ip来统计访问最多的前10个视频 <video,[ip1,ip2...]>
  * 需求3：统计一天中每个小时的流量 <00,34G>
  *
  *
  * @Created by tzq 2018/6/14 15:52
  **/
object Demo2 {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Demo2")
      .master("local[2]")
      .getOrCreate()

    val ipPattern = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)".r
    val rdd =spark.sparkContext.textFile("file:///Users/tanzhengqiang/data/spark/cdn.txt")

//   ### 1.
//   rdd.flatMap(line => (ipPattern.findFirstIn(line))).map(k => (k,1)).reduceByKey(_ + _)
//     .sortBy(_._2,false)
//     .take(10)
//     .foreach(println(_))



    // 2.
//    val videoPattern = "(\\d+).mp4".r
//    def getFileNameAndIp(line:String)={
//      (videoPattern.findFirstIn(line),ipPattern.findFirstIn(line))
//    }
//
//    val res =rdd.filter(line => (line.matches(".*([0-9]+)\\.mp4.*")))
//        .map(x =>getFileNameAndIp(x))
//        .groupByKey()
//        .map(x =>(x._1,x._2.toList.distinct))
//        .sortBy(_._2.size,false)
//      .take(10)
//        .foreach(x => println("video:"+ x._1 + ",IP num:" + x._2.size))


   //3.
//    val datePatern = "(\\d{4}):(\\d{2}):(\\d{2})".r
//    //匹配 http 响应码和请求数据大小
//    val httpSizePattern=".*\\s(200|206|304)\\s([0-9]+)\\s.*".r


    //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
    val  timePattern=".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r
    //匹配 http 响应码和请求数据大小
    val httpSizePattern=".*\\s(200|206|304)\\s([0-9]+)\\s.*".r


    def isMatch(pattern:Regex,str:String)={
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }
//    def getTimeAndSize(x: String)={
//      val hour = datePatern.findFirstIn(x).toString.split(":")(1)
//      val size = httpSizePattern.findFirstIn(x).toString.split("\\s+")(1)
//      //(请求小时,请求大小)
//      (hour,size)
//    }

    /**
      * 获取日志中小时和http 请求体大小
      * @param line
      * @return
      */
    def getTimeAndSize(line:String)={
      var res=("",0L)
      try{
        val  httpSizePattern(code,size)=line
        val  timePattern(year,hour)=line
        res=(hour,size.toLong)
      }catch {
        case ex:Exception  => ex.printStackTrace()
      }
      res
    }

//    rdd.filter(x=>isMatch(httpSizePattern,x))
//        .filter(x=>isMatch(datePatern,x))
//        .map(x =>getTimeAndSize(x)).take(10).foreach(println(_))
//        .groupByKey()
//        .map(x => (x._1,x._2.sum))
//        .sortByKey()
//        .foreach(x => println(x._1+"时，CDN流量："+ (x._2.toInt)/(1024*1024*1024)+"G"))




    rdd.filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)).map(x=>getTimeAndSize(x)).groupByKey()
      .map(x=>(x._1,x._2.sum)).sortByKey().foreach(x=>println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"G"))


//      16时 CDN流量=45G
//      17时 CDN流量=44G
//      18时 CDN流量=45G
//      19时 CDN流量=51G
//      20时 CDN流量=55G
//      21时 CDN流量=53G
//      22时 CDN流量=42G
//      23时 CDN流量=25G

    spark.stop()
  }

}
