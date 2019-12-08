package com.kafkaToSparkToCass.model

case class PayLoad (
                   userId: String,
                   eventTime: Long
                   )