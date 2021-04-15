package cloud.maxzhao.adClicks_analysis

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 简单计数
 * @author Max
 * @date 2021/4/14 17:02
 */
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
