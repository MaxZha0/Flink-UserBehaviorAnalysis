package cloud.maxzhao.hotItems

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 一小时内热门物品分析，5min更新一次
 * 输出Top N
 * @author Max
 * @date 2021/4/13 22:43
 */
//数据样例类定义
case class UserBehavior(userId : Long, itemId : Long, categoryId : Int, behavior : String, timestamp : Long)
//筛选后输出类型
case class ItemViewCount( itemId : Long, windowEnd : Long, count : Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    //指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\a_HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt,
          dataArray(3).trim, dataArray(4).trim.toLong )
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    //一小时内热门商品，设定是只看pv
    val processedStream = dataStream
      .filter(_.behavior == "pv") //设定是只看pv
      .keyBy(_.itemId)  //按物品id排序
      .timeWindow(Time.hours(1), Time.minutes(5)) //滑动窗口，5min
      //.aggregate(预聚合函数+窗口函数)
      .aggregate(new CountAgg(), new WindowResult())  //计数，取结果
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(5))  //排序

    processedStream.print().setParallelism(1)

    env.execute("HotItems")
  }

}



