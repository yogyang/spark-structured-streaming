package com.kafkaToSparkToCass

/**
  * Created by ankur on 18.12.16.
  */
import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.time.LocalTime

import com.datastax.driver.core.Session
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.kafkaToSparkToCass.model.{PayLoad, RawMsgEvent, UserMsgState}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

object Main {

  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

    logger.setLevel(Level.INFO)

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.getStackTrace.toString)
    }
  }

  def convertStateToPayLoad(
                           state: UserMsgState
                           ): (Iterator[PayLoad], Long) = {
    logger.info("convert state to payload for state: " + state)
    val count = state.eventCount / 5
    val payloads = (0L until count).map{
      i => PayLoad(state.userId, state.maxTimeMs)
    }.toIterator

    (payloads, count)

  }

  def updateStateFn(
                   userId: String,
                   events: Iterator[RawMsgEvent],
                   state: GroupState[UserMsgState]
                   ): Iterator[PayLoad] = {
    if (state.hasTimedOut) {
      val finalState = state.get
      logger.info("session timeout for userId " + userId)
      state.remove()

      //Iterator(finalState)
      convertStateToPayLoad(finalState)._1
    } else {
      val currentState = state.getOption
      val aggregatedState = events.foldLeft(currentState)(
        (state, event) => {
          state.orElse{
            logger.info("created new state as no state for userid " + userId)
            Some(UserMsgState(event.userId, event.time.getTime, 0))
          }.map{
            s => s.increaseEventCount()
          }.map{
            s => s.updateMaxTimeMs(event.time.getTime)
          }
        }
      ).get

      val (payLoads, payLoadCount) = convertStateToPayLoad(aggregatedState)
      state.setTimeoutTimestamp(aggregatedState.maxTimeMs, "2 minutes")
      state.update(aggregatedState.decrementBy(5 * payLoadCount))
      //Iterator(aggregatedState)
      logger.info("Payload lenth for this batch:" + payLoadCount)
      logger.info("updated state:" + state.get)
      payLoads

    }
  }
}


class SparkJob extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  logger.setLevel(Level.INFO)
  val sparkSession =
    SparkSession.builder
      .master("local[*]")
      .appName("kafka2Spark")
      .getOrCreate()


  def runJob() = {

    logger.info("Execution started with following configuration")
    val cols = List("user_id", "time", "event")

    import sparkSession.implicits._

    val streamDF = sparkSession.readStream
      .format("kafka")
      .option("subscribe", "test.1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
//    streamDF.printSchema()

    val lines = streamDF
      .selectExpr("CAST(value AS STRING)",
                  "CAST(topic as STRING)",
                  "CAST(partition as INTEGER)")
      .as[(String, String, Integer)]


    val df =
      lines.map {
        line => {
          // value being sent out as a comma separated value "userid_1;2015-05-01T00:00:00;some_value"
          val columns = line._1.split(";")
          RawMsgEvent(columns(0), Commons.getTimeStamp(columns(1)), columns(2))

        }
      }.as[RawMsgEvent]

//    df.printSchema()

    import com.kafkaToSparkToCass.Main._

    val finalDF = df
      .withWatermark("time", "3 seconds")
      .groupByKey(_.userId)
      .flatMapGroupsWithState(OutputMode.Complete(), GroupStateTimeout.EventTimeTimeout())(updateStateFn)

    val consoleOutput = finalDF.writeStream
      .queryName("testStreaming")
      .format("console")
      .start()

    consoleOutput.awaitTermination()


  }
}
