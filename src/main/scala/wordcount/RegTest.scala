package wordcount

import scala.util.matching.Regex

/**
  * @Desc  正则
  *
  *      https://www.cnblogs.com/richiewlq/p/7307581.html
  * @Created by tzq 2018/6/14 16:07
  **/
object RegTest {
  def main(args: Array[String]): Unit = {
    val str = "223.93.159.226 HIT 203 [15/Feb/2017:11:14:35 +0800] GET htt.1 206 5440 (Windows NT 6.ent/7.0; rv:11.0) like Gecko Core/1.53.2141.400 QQBrowser/9.5.10219.400"

//    val pattern = "(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})".r
//    val ip = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)".r
//
//
//    println(pattern findFirstIn(str))

    val  timePattern(year,hour)=str
         println(year,hour)


  }
  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val  timePattern=".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r
  //匹配 http 响应码和请求数据大小
  val httpSizePattern=".*\\s(200|206|304)\\s([0-9]+)\\s.*".r


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

}

