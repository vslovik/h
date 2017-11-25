package org.unipi.matrixnorm

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import java.io.IOException
import java.util
import org.apache.hadoop.io.{NullWritable, Text}

class HadoopMatrixGeneratorInputFormat extends FileInputFormat[Text, NullWritable] {

  @throws[IOException]
  override def getSplits(job: JobContext): util.List[InputSplit] = {

    val numSplits: Int = job.getConfiguration.getInt(HadoopMatrixGeneratorInputFormat.NUM_MAP_TASKS, -1)

    // Create a number of input splits equivalent to the number of tasks
    val splits: util.ArrayList[InputSplit] = new util.ArrayList[InputSplit]

    for (i <- 0 to numSplits) {
      splits.add(new FakeInputSplit())
    }

    splits
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, NullWritable] = {
    val rr = new HadoopMatrixGeneratorRecordReader
    rr.initialize(split, context)
    rr
  }

}

object HadoopMatrixGeneratorInputFormat {

  val NUM_MAP_TASKS = "random.generator.map.tasks"
  val NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task"

  def setNumMapTasks(job: Job, i: Int): Unit = {
    job.getConfiguration.setInt(NUM_MAP_TASKS, i)
  }

  def setNumRecordsPerTask(job: Job, i: Int): Unit = {
    job.getConfiguration.setInt(NUM_RECORDS_PER_TASK, i)
  }
}
