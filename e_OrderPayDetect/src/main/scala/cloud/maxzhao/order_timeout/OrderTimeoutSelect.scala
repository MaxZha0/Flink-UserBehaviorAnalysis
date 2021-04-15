package cloud.maxzhao.order_timeout

import org.apache.flink.cep.PatternTimeoutFunction
import java.util
/**
 * 自定义超时事件序列处理函数
 *
 * @author Max
 * @date 2021/4/15 11:23
 */

class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent, OrderResult] {

  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {

    val timeoutOrderId = map.get("begin").iterator().next().orderId
//没有匹配到，所以是空指针
//    println("pay "+map.get("pay").iterator().next().orderId)
    println(" 超时时间" + (l - map.get("begin").iterator().next().eventTime * 1000L))
    OrderResult(timeoutOrderId, "timeout")
  }
}
