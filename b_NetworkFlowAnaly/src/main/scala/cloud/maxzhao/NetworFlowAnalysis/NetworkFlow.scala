package cloud.maxzhao.NetworFlowAnalysis

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

import java.text.SimpleDateFormat

/**
 * 10min热门页面统计，每 5s更新一次
 * 输出Top N
 * @author Max
 * @date 2021/4/14 11:48
 */
//源数据类型
case class ApacheLogEvent(ip : String, userId : String, eventTime: Long, method : String, url: String)
//半成品数据类型
case class UrlViewCount(url : String, windowEnd : Long, count : Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 3 )
    //指定时间语义
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val dataStream = env.readTextFile( "F:\\IDEA_project\\电商用户行为分析\\b_NetworkFlowAnaly\\src\\main\\resources\\apache.log" )
      .map( data => {
        val dataArray = data.split( " " )
        //转换时间
        val dataFormat = new SimpleDateFormat( "dd/MM/yyyy:HH:mm:ss" )
        //直接转换为时间戳
        val timestamp = dataFormat.parse( dataArray(3).trim ).getTime
        ApacheLogEvent( dataArray(0).trim, dataArray(1).trim, timestamp,
          dataArray(5).trim, dataArray(6).trim )
      } )
        //乱序数据,数据乱序比较大，watermark先不调大
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
            override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
          })

    val sortedStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      //允许60s得迟到数据
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNURLs(5))


  sortedStream.print()

    env.execute("network-flow")
  }
}
