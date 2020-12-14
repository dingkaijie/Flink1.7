package com.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool = ParameterTool.fromArgs(args)

    val port : Int = try
      parameterTool.get("port").toInt
    catch {
      case e: Exception => {
        print("error")
      }
        9999
    }

    import  org.apache.flink.api.scala._

    val text = env.socketTextStream("beifeng04.com",port,'\n')

    val flattext = text.flatMap(line => line.split("\\s"))
      .map(x => Word(x,1))
      .keyBy("name")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")
    flattext.print().setParallelism(1)

    env.execute("WordCountScala")

  }

  case class  Word(name : String ,count: Long)



}
