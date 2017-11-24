package org.unipi.matrixnorm

import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.RecordReader
import java.io.IOException

import org.apache.hadoop.io.Text
import org.scalacheck.Gen

class HadoopMatrixGeneratorInputFormat extends FileInputFormat[Integer, Text] {

  @throws[IOException]
  @throws[InterruptedException]
  override def createRecordReader(var1: InputSplit, var2: TaskAttemptContext): RecordReader[Integer, Text] = {
    new RecordReader[Integer, Text] {

      @throws[IOException]
      override def close(): Unit = {
      }

      val mg = new MatrixGenerator
      val r = new scala.util.Random

      var currentKey = 0
      var currentValue = new Text("")

      val records = 100 //ToDo

      @throws[IOException]
      @throws[InterruptedException]
      override def getCurrentKey: Integer = currentKey

      @throws[IOException]
      @throws[InterruptedException]
      override def getCurrentValue: Text = {
        if (currentValue == new Text("")) nextKeyValue
        currentValue
      }

      @throws[IOException]
      @throws[InterruptedException]
      override def initialize(arg0: InputSplit, arg1: TaskAttemptContext): Unit = {
      }

      @throws[IOException]
      @throws[InterruptedException]
      override def getProgress = 0f

      @throws[IOException]
      @throws[InterruptedException]
      override def nextKeyValue: Boolean = {
        if (currentKey < records) {
          val rows = r.nextInt(5)
          val cols = r.nextInt(5)
          val limit = r.nextInt(100)
          currentKey += 1
          currentValue = new Text(mg.serialize(mg.matrix(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols).sample))
          return true
        }
        false
      }

    }

  }
}
