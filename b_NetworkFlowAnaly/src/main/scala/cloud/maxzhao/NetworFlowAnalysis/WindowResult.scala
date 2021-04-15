package cloud.maxzhao.NetworFlowAnalysis

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *自定义窗口函数，输出结果
 * 包装每5min内的数据
 * @author Max
 * @date 2021/4/13 23:34
 */
class WindowResult extends WindowFunction[Long, UrlViewCount,String, TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[UrlViewCount]): Unit = {

    out.collect(UrlViewCount(key,window.getEnd, input.iterator.next()))
  }
}
