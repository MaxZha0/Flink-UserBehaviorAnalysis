package cloud.maxzhao.pay_detect

import cloud.maxzhao.order_timeout.OrderEvent
import cloud.maxzhao.pay_detect.TxMatchDetect.{unmatchedPays, unmatchedReceipts}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
 *
 * @author Max
 * @date 2021/4/15 12:09
 */
private class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 定义状态来保存已经到达的订单支付事件和到账事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

  // 订单支付事件数据的处理
  override def processElement1(pay: OrderEvent,
                               ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 判断有没有对应的到账事件
    val receipt = receiptState.value()
    if( receipt != null ){
      // 如果已经有receipt，在主流输出匹配信息，清空状态
      out.collect((pay, receipt))
      receiptState.clear()
    } else {
      // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )
    }
  }

  // 到账事件的处理
  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 同样的处理流程
    val pay = payState.value()
    if( pay != null ){
      out.collect((pay, receipt))
      payState.clear()
    } else {
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 5000L )
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 到时间了，如果还没有收到某个事件，那么输出报警信息
    if( payState.value() != null ){
      // recipt没来，输出pay到侧输出流
      ctx.output(unmatchedPays, payState.value())
    }
    if( receiptState.value() != null ){
      ctx.output(unmatchedReceipts, receiptState.value())
    }
    payState.clear()
    receiptState.clear()
    }


}
