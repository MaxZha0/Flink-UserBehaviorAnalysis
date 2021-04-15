package cloud.maxzhao.hotItems

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/**
 * 排序功能
 * @author Max
 * @date 2021/4/14 10:56
 */
class TopNHotItems(topSize : Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  private var itemState : ListState[ItemViewCount] = _
  override def processElement(i: ItemViewCount,
                              context: KeyedProcessFunction[Long,
                                ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    //每条数据存入状态
    itemState.add(i)
    //注册定时器
    context.timerService().registerEventTimeTimer( i.windowEnd + 1 )
  }

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }

  override def close(): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
    itemState.clear()
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))


    //定时器到时，取出所有输出
    val allItems : ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]

    itemState.get().forEach(item => allItems += item)

    //按照count排序,默认升序，reverse一下
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    itemState.clear()

    //结果输出格式化
    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //输出每一个商品
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No.").append(i+1).append(": ")
        .append("商品Id= ").append(currentItem.itemId)
        .append("浏览量= ").append(currentItem.count)
        .append("\n")
    }
    result.append("======================================")
    Thread.sleep(1000)
    out.collect(result.toString())

  }
}
