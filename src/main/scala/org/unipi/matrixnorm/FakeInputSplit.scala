package org.unipi.matrixnorm

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.Writable
import java.io._

class FakeInputSplit extends InputSplit with Writable {

  @throws[IOException]
  def readFields(var1: DataInput): Unit = {

  }

  @throws[IOException]
  def write(var1: DataOutput): Unit = {

  }

  @throws[IOException]
  @throws[InterruptedException]
  def getLength: Long = {
    0L
  }

  @throws[IOException]
  @throws[InterruptedException]
  def getLocations = new Array[String](0)

}