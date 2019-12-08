package com.kafkaToSparkToCass.model

import java.sql.Timestamp

case class RawMsgEvent(
                      userId: String,
                      time: Timestamp,
                      event: String
                      )
