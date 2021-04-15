package cloud.maxzhao.order_timeout

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 检测创建订单后，15min之后 支付的用户
 * cep模式匹配
 *
 * @author Max
 * @date 2021/4/15 10:42
 */
// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeoutWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单数据
    val orderEventStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\e_OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //2、定义匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //3、应用模式
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    //4、调用select，提取
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    //5、输出正流和侧流
    val resultStream = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect, new OrderPaySelect)

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }


}
