package org.unipi.matrixgen

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configured

object MatrixGenDriver extends Configured with Tool {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val res = ToolRunner.run(new Configuration(), MatrixGenDriver, args)
    System.exit(res)
  }

  @throws[Exception]
  def run(args: Array[String]): Int = {
    if (args.length != 3) {
      System.err.println("USAGE : [number of map tasks] [number of records per task] [output path]")
      1
    } else {

      val configuration = new Configuration

      val numMapTasks: Int = BigInt(args(0)).intValue()
      val numRecordsPerTasks = BigInt(args(1)).intValue()
      val outputDir = new Path(args(2))

      val job = Job.getInstance(configuration, "matrix generator")

      FileSystem.get(outputDir.toUri, configuration).delete(outputDir, true)

      job.setJarByClass(this.getClass)
      job.setNumReduceTasks(0)
      job.setInputFormatClass(classOf[MatrixGenInputFormat])

      job.setInputFormatClass(classOf[MatrixGenInputFormat])

      MatrixGenInputFormat.setNumMapTasks(job, numMapTasks)
      MatrixGenInputFormat.setNumRecordsPerTask(job, numRecordsPerTasks)

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      FileOutputFormat.setOutputPath(job, outputDir)

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[NullWritable])

      if (job.waitForCompletion(true)) 0
      else 1
    }
  }
}