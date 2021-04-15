package cloud.maxzhao.NetworFlowAnalysis

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
class TopNURLs(topSize : Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  private var urlState : ListState[UrlViewCount] = _
  override def processElement(i: UrlViewCount,
                              context: KeyedProcessFunction[Long,
                                UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    //每条数据存入状态
    urlState.add(i)
    //注册定时器
    context.timerService().registerEventTimeTimer( i.windowEnd + 1 )
  }

  override def open(parameters: Configuration): Unit = {
    urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))
  }

  override def close(): Unit = {
    urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))
    urlState.clear()
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))


    //定时器到时，取出所有输出
    val allURLs : ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]

    urlState.get().forEach(url => allURLs += url)

    //按照count排序,默认升序，reverse一下
    val sortedURLs = allURLs.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    urlState.clear()

    //结果输出格式化
    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //输出每一个商品
    for(i <- sortedURLs.indices){
      val currentURL = sortedURLs(i)
      result.append("No.").append(i+1).append(": ")
        .append("URL=").append(currentURL.url)
        .append(" 访问量= ").append(currentURL.count)
        .append("\n")
    }
    result.append("======================================")
    Thread.sleep(1000)
    out.collect(result.toString())

  }
}
