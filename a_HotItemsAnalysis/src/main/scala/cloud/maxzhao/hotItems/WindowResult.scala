package cloud.maxzhao.hotItems

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *自定义窗口函数，输出结果
 * 包装每30s内的数据
 * @author Max
 * @date 2021/4/13 23:34
 */
class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {

    out.collect(ItemViewCount(key,window.getEnd, input.iterator.next()))
  }
}
