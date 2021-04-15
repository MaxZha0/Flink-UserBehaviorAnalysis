package cloud.maxzhao.adClicks_analysis

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 广告统计信息分析
 * 根据省分做分类聚合
 * 过滤刷单行为
 *
 * @author Max
 * @date 2021/4/14 16:56
 */

// 输入的广告点击事件样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
// 按照省份统计的输出结果样例类
case class CountByProvince( windowEnd: String, province: String, count: Long )
// 输出的黑名单报警信息
case class BlackListWarning( userId: Long, adId: Long, msg: String )

object AdStatisticsByGeo {

  // 定义侧输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据并转换成AdClickEvent
    val adEventStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\c_MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        AdClickEvent( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 自定义process function，过滤大量刷点击的行为，在测输出流输出
    val filterBlackListStream = adEventStream
      //两个key的筛选
      .keyBy( data => (data.userId, data.adId) )
      .process( new FilterBlackListUser(100) )


    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountResult() )

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")

    env.execute("ad statistics job")

  }
}
