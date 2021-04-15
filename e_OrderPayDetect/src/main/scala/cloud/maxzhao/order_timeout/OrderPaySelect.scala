package cloud.maxzhao.order_timeout

import org.apache.flink.cep.PatternSelectFunction
import java.util
/**
 * 自定义正常支付事件序列处理函数
 *
 * @author Max
 * @date 2021/4/15 11:24
 */

class OrderPaySelect extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
