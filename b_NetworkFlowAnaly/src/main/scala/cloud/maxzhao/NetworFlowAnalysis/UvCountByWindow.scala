package cloud.maxzhao.NetworFlowAnalysis

import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *用于过略相同IP
 * 问题： 在内存中的set进行去重，容易内存溢出 ***********
 *
 * @author Max
 * @date 2021/4/14 15:22
 */
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow,
                     input: Iterable[UserBehavior],
                     out: Collector[UvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var idSet = Set[Long]()
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for( userBehavior <- input ){
      idSet += userBehavior.userId
    }
    out.collect( UvCount( window.getEnd, idSet.size ) )
  }
}
