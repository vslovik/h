package org.unipi.matrixnorm

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object HadoopMatrixNorm {

  class MatrixNormMapper extends Mapper[Integer, Text, Text, IntWritable] {

    override def map(key: Integer, value: Text, context: Mapper[Integer, Text, MapWritable, ArrayWritable]#Context): Unit = {
      val matrix = (new MatrixGenerator).deserialize(value.toString)
      val rows = matrix.length
      val cols = matrix.head.length
      var i = 0
      while (i < matrix.length) {

        val vl = new MapWritable
        val nn  = new IntWritable(key)
        val ii =  new IntWritable(i)
        vl.put(nn, ii)

        matrix.foreach {
          row => {
            val mm = row.map{ e: Double => new DoubleWritable(e)}
            val v = new ArrayWritable(DoubleWritable.class, mm)
            context.write(vl, v) //Add Matrix key
          }

        }
        i += 1
      }
    }
  }

  class MatrixNormReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    }
  }

  class MatrixNormCombiner extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      context.write(key, new IntWritable("ToDo"))
    }
  }

  class MatrixNormComposer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      context.write(key, new IntWritable("ToDo"))
    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"matrix normalization")
    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[MatrixNormMapper])
    job.setCombinerClass(classOf[MatrixNormCombiner])
    job.setReducerClass(classOf[MatrixNormReducer])
    job.setReducerClass(classOf[MatrixNormComposer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}



