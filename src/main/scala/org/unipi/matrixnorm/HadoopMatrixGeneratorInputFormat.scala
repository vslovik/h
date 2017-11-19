package org.unipi.matrixnorm

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.LongWritable
import java.io.IOException

class HadoopMatrixGeneratorInputFormat extends FileInputFormat {

  @throws[IOException]
  @throws[InterruptedException]
  override def createRecordReader(var1: InputSplit, var2: TaskAttemptContext): HadoopMatrixLineRecordReader = {
    new HadoopMatrixLineRecordReader()
  }
}
