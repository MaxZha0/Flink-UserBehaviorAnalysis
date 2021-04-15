package cloud.maxzhao.loginFailed_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 用cep来匹配模式.短时间内连续登陆失败的用户
 *
 * @author Max
 * @date 2021/4/14 17:29
 */

// 输出的异常报警信息样例类
case class Warning3( userId: Long, firstFailTime: Long, nextFailTime: Long, lastFailTime: Long, warningMsg: String)
object LoginFailWithCep {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取事件数据，创建简单事件流
    val loginEventStream = env.readTextFile("F:\\IDEA_project\\电商用户行为分析\\d_LoginFailedDetect\\src\\main\\resources\\LoginLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )
      .keyBy(_.userId)

    // 2. 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      //严格近邻
      .next("2ed").where(_.eventType == "fail")
      .next("3th").where(_.eventType == "fail")
      .within(Time.seconds(5))

    // 3. 在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    // 4. 从pattern stream上应用select function，检出匹配事件序列
    val loginFailDataStream = patternStream.select( new LoginFailMatch() )

    loginFailDataStream.print()

    env.execute("login fail with cep job")
  }
}


class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning3]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning3 = {
    // 从map中按照名称取出对应的事件

    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("3th").iterator().next()
    val nextFail = map.get("2ed").iterator().next()
    Warning3( firstFail.userId, firstFail.eventTime, nextFail.eventTime, lastFail.eventTime, "login fail!" )
  }
}
