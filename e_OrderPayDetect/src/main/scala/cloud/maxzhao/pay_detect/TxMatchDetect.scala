package cloud.maxzhao.pay_detect

import cloud.maxzhao.order_timeout.OrderEvent
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 *
 * 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是
否到账了。而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来
做 合 并 处 理 。 这 里 我 们 利 用 connect 将 两 条 流 进 行 连 接 ， 然 后 用 自 定 义 的
CoProcessFunction 进行处理。

 * @author Max
 * @date 2021/4/15 11:44
 */
// 定义接收流事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatchDetect {
  // 定义侧数据流tag
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单事件流
    val orderEventStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\e_OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      //只接受支付成功
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptEventStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\e_OrderPayDetect\\src\\main\\resources\\ReceiptLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent( dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process( new TxPayMatch )

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchReceipts")

    env.execute("tx match job")

  }

}
