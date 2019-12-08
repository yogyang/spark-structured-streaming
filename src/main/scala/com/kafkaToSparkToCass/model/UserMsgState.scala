package com.kafkaToSparkToCass.model

case class UserMsgState (
                        userId: String,
                        maxTimeMs: Long,
                        eventCount: Long
                        ){
  def updateMaxTimeMs(timeMs: Long): UserMsgState =
    if (timeMs > this.maxTimeMs) copy(maxTimeMs = timeMs) else this

  def increaseEventCount(): UserMsgState = copy(eventCount = this.eventCount + 1)

  def decrementBy(n: Long): UserMsgState = copy(eventCount = this.eventCount - n)
}
