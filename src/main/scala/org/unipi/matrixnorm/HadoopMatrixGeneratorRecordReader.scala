package org.unipi.matrixnorm

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.io._
import org.scalacheck.Gen

class HadoopMatrixGeneratorRecordReader extends RecordReader[Text, NullWritable] {

  private var numRecordsToCreate = 0
  private var createdRecords = 0

  private val minRows = 1
  private val minCols = 1

  private val maxRows = 5
  private val maxCols = 5

  private val minValue = 1
  private val maxValue = 100

  private val key = new Text
  private val value = NullWritable.get

  private val mg = new MatrixGenerator
  private val r = new scala.util.Random

  @throws[IOException]
  @throws[InterruptedException]
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    // Get the number of records to create from the configuration
    this.numRecordsToCreate = context.getConfiguration.getInt(HadoopMatrixGeneratorInputFormat.NUM_RECORDS_PER_TASK, -1)
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def nextKeyValue: Boolean = {
    // If we still have records to create
    if (createdRecords < numRecordsToCreate) {
      val rows = minRows + r.nextInt(maxRows - minRows)
      val cols = minCols + r.nextInt(maxCols - minCols)
      val limit = minValue + r.nextInt(maxValue - minValue)
      val serializedMatrix = mg.serialize(mg.matrix(Gen.choose(0.0, limit))(rows, cols).sample)
      key.set(serializedMatrix)
      createdRecords += 1
      return true
    }
    // We are done creating records
    false
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def getCurrentKey: Text = key

  @throws[IOException]
  @throws[InterruptedException]
  override def getCurrentValue: NullWritable = value

  @throws[IOException]
  @throws[InterruptedException]
  override def getProgress: Float = createdRecords.toFloat / numRecordsToCreate.toFloat

  @throws[IOException]
  override def close(): Unit = {
    // nothing to do here...
  }

}
