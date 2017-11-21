// https://www.programcreek.com/java-api-examples/index.php?api=org.apache.hadoop.io.ArrayWritable
package org.unipi.matrixnorm

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.ObjectWritable

class Mapper1Key(val matrixIndex: Integer, val rowIndex: Integer)
class Mapper2Key(val matrixIndex: Integer, val colIndex: Integer, val flag: Boolean)
class Mapper2Value(val rowIndex: Integer, val colValue: Double)

object HadoopMatrixNorm {

  class MatrixNormMapper1 extends Mapper[Integer, Text, ObjectWritable, ArrayWritable] {

    override def map(key: Integer, value: Text, context: Mapper[Integer, Text, ObjectWritable, ArrayWritable]#Context): Unit = {

      val matrix = (new MatrixGenerator).deserialize(value.toString)

//      val rows = matrix.length
//      val cols = matrix.head.length

      for (index <- matrix.indices) {
        context.write(
          new ObjectWritable(new Mapper1Key(key, index)),
          new ArrayWritable(
            classOf[DoubleWritable],
            matrix(index).map { e: Double => new DoubleWritable(e) }.toArray
          )
        )
      }
    }
  }

  class MatrixNormMapper2 extends Mapper[ObjectWritable, ArrayWritable, ObjectWritable, ObjectWritable] {

    override def map(key: ObjectWritable, value: ArrayWritable, context: Mapper[ObjectWritable, ArrayWritable, ObjectWritable, ObjectWritable]#Context): Unit = {

      val row = value.get()

      for (i <- row.indices) {

        val k = key.get() match { case j: Mapper1Key => j}
        val v = row(i) match { case h: DoubleWritable => h }

        context.write(
          new ObjectWritable(new Mapper2Key(k.matrixIndex, i, false)),
          new ObjectWritable(new Mapper2Value(k.rowIndex, v.get()))
        )

        context.write(
          new ObjectWritable(new Mapper2Key(k.matrixIndex, i, true)),
          new ObjectWritable(new Mapper2Value(k.rowIndex, v.get()))
        )

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

    job.setMapperClass(classOf[MatrixNormMapper1])
    job.setMapperClass(classOf[MatrixNormMapper2])
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



