package cloud.maxzhao.adClicks_analysis

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 *
 * @author Max
 * @date 2021/4/14 17:02
 */
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[CountByProvince]): Unit = {
    out.collect( CountByProvince( new Timestamp(window.getEnd).toString, key, input.iterator.next() ) )
  }
}
