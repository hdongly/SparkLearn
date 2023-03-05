package com.apple.udf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

case class Buff(var sum: Long, var cnt: Long)

class MyAvgAgeUDAF extends Aggregator[Long, Buff, Double]{
  override def zero: Buff = Buff(0,0)

  override def reduce(b: Buff, a: Long): Buff = {
    b.sum += a
    b.cnt += 1
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.cnt += b2.cnt
    b1
  }

  override def finish(reduction: Buff): Double = {
    reduction.sum.toDouble/reduction.cnt
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}