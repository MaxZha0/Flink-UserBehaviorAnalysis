package cloud.maxzhao.NetworFlowAnalysis

import org.apache.flink.api.common.functions.AggregateFunction

/**
 *预聚合，只起计数功能
 *
 * @author Max
 * @date 2021/4/13 23:28
 */
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  //acc 为计数器
  override def createAccumulator(): Long = 0L
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
  override def getResult(acc: Long): Long = acc
  //合并时候如何聚合
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
