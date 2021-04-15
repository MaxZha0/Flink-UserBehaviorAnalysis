package cloud.maxzhao.market_analysis

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 计算app推广渠道,每1h，滑动10min
 *
 * @author Max
 * @date 2021/4/14 16:18
 */
//输入数据样例类
case class MarketingUserBehavior(userId : String, behavior : String, channel : String, timestamp : Long)
// 输出结果样例类
case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )

object AppCountByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")  //去除卸载数据
      .keyBy(_.channel)
      //其他数据并不重要
      .map( data => ( (data.channel, data.behavior), 1L ) )
      .keyBy(_._1)  //以渠道和行为作为分类
      .timeWindow(Time.hours(1),Time.minutes(10))
      .process(new MarketingCountByChannel())

    dataStream.print()
    env.execute("app marketing by channel job")
  }

}
