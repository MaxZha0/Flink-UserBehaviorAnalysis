package cloud.maxzhao.pay_detect
import cloud.maxzhao.order_timeout.OrderEvent
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 ** 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是
否到账了。而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来
做 合 并 处 理 。
 * ##################################################
 * 这里用joinfunction进行检测
 * 但是只能检测对应数据，不对应数据无法输出
 *
 * @author Max
 * @date 2021/4/15 12:18
 */
object TxMatchByJoin {
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

      // join处理
      val processedStream = orderEventStream.intervalJoin( receiptEventStream )
        .between(Time.seconds(-5), Time.seconds(5))
        .process( new TxPayMatchByJoin )

      processedStream.print()

      env.execute("tx pay match by join job")

  }
}



