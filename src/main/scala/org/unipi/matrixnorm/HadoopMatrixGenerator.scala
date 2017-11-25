package org.unipi.matrixnorm

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

object HadoopMatrixGenerator {

  class MatrixMapper extends Mapper[Text, Text, Text, Text] {
    override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
        context.write(key, value)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("USAGE : [number of map tasks] [number of records per task] [output path]")
      System.exit(0)
    } else {

      val configuration = new Configuration

      val numMapTasks: Int = args(0).toInt
      val numRecordsPerTasks = args(1).toInt
      val outputDir = new Path(args(2))

      val job = Job.getInstance(configuration, "matrix generator")

      // recursively delete the data set if it exists.
      FileSystem.get(outputDir.toUri, configuration).delete(outputDir, true)

      job.setJarByClass(this.getClass)

      job.setNumReduceTasks(0)

      job.setInputFormatClass(classOf[HadoopMatrixGeneratorInputFormat])

      HadoopMatrixGeneratorInputFormat.setNumMapTasks(job, numMapTasks)
      HadoopMatrixGeneratorInputFormat.setNumRecordsPerTask(job, numRecordsPerTasks)

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      FileOutputFormat.setOutputPath(job, outputDir)

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[NullWritable])

      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
  }
}

