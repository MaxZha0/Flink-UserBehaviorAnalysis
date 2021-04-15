package cloud.maxzhao.market_analysis

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * 随机产生数据源
 *
 * @author Max
 * @date 2021/4/14 16:22
 */
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  // 定义是否运行的标识位
  var running = true
  // 定义用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义渠道的集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义一个随机数发生器
  val rand: Random = new Random()

  override def cancel(): Unit =
    running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L

    // 随机生成所有数据
    while( running && count < maxElements ){
      //随机uuid作为UserID
      val id = UUID.randomUUID().toString
      //随机5选1
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()

      ctx.collect( MarketingUserBehavior( id, behavior, channel, ts ) )

      count += 1
      //暂停10ms
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }
}
