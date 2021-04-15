package cloud.maxzhao.NetworFlowAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 统计1小时页面的IP 独特访问
  * *：过滤相同IP的访问
 * 问题： 在内存中的set进行去重，容易内存溢出
 *
  * @author Max
  */
case class UvCount( windowEnd: Long, uvCount: Long )

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\a_HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter( _.behavior == "pv" )    // 只统计pv操作
      //过滤相同IP的访问
      .timeWindowAll( Time.hours(1) )
      .apply( new UvCountByWindow() )

    dataStream.print()
    env.execute("uv job")
  }
}


