package org.unipi.matrixgen

import java.io.IOException
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, JobContext, InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

class MatrixGenInputFormat extends FileInputFormat[Text, NullWritable] {

  @throws[IOException]
  override def getSplits(job: JobContext): java.util.List[InputSplit] = {

    val numSplits: Int = job.getConfiguration.getInt(MatrixGenInputFormat.NUM_MAP_TASKS, -1)
    val splits: java.util.ArrayList[InputSplit] = new java.util.ArrayList[InputSplit]
    for (i <- 1 to numSplits) {
      splits.add(new FakeInputSplit())
    }

    splits
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, NullWritable] = {
    val rr = new MatrixGenRecordReader
    rr.initialize(split, context)
    rr
  }
}

object MatrixGenInputFormat {

  val NUM_MAP_TASKS = "random.generator.map.tasks"
  val NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task"

  def setNumMapTasks(job: Job, i: Int): Unit = {
    job.getConfiguration.setInt(NUM_MAP_TASKS, i)
  }

  def setNumRecordsPerTask(job: Job, i: Int): Unit = {
    job.getConfiguration.setInt(NUM_RECORDS_PER_TASK, i)
  }
}
