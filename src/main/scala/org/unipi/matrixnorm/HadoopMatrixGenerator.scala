package org.unipi.matrixnorm

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters._

object HadoopMatrixGenerator {

  class MatrixMapper extends Mapper[Text, Text, Text, Text] {
    override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
        context.write(key, value)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("USAGE : [number of records] [output path]")
      System.exit(0)
    } else {
      val configuration = new Configuration
      val job = Job.getInstance(configuration, "matrix generator")
      val outputDir = new Path(args(1))

      // recursively delete the data set if it exists.
      FileSystem.get(outputDir.toUri, configuration).delete(outputDir, true)

      job.setJarByClass(this.getClass)
      job.setMapperClass(classOf[MatrixMapper])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[Text])

      job.setInputFormatClass(classOf[HadoopMatrixGeneratorInputFormat])
      job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

      FileOutputFormat.setOutputPath(job, outputDir)
      System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
  }
}

