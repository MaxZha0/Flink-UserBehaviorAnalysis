package cloud.maxzhao.loginFailed_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 *对于网站而言，用户登录并不是频繁的业务操作。如果一个用户短时间内频繁
登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。因此我们考虑，
应该对用户的登录失败动作进行统计，具体来说，如果同一用户（可以是不同 IP）
在 2 秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行
报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
 *
 * 半成品，有若干BUG
 *
  */

// 输入的登录事件样例类
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
// 输出的异常报警信息样例类
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取事件数据
    val loginEventStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\d_LoginFailedDetect\\src\\main\\resources\\LoginLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )

    val warningStream = loginEventStream
      .keyBy(_.userId)    // 以用户id做分组
      .process( new LoginWarning(2) )

    warningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {

    if( value.eventType == "fail" ){
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if( iter.hasNext ){
        // 如果已经有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if( value.eventTime < firstFail.eventTime + 2 ){
          // 如果两次间隔小于2秒，输出报警
          out.collect( Warning( value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds." ) )
        }
        // 更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }




}
