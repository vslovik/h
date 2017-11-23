// https://www.programcreek.com/java-api-examples/index.php?api=org.apache.hadoop.io.ArrayWritable
// https://stackoverflow.com/questions/20212884/mapreduce-combiner
// https://stackoverflow.com/questions/27404696/scala-for-loop-and-iterators
package org.unipi.matrixnorm

import java.{lang, util}

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
import org.apache.hadoop.io.SortedMapWritable

class Mapper1Key(val matrixIndex: Integer, val rowIndex: Integer)

class Mapper2Key(val matrixIndex: Integer, val colIndex: Integer, val flag: Boolean)
class Mapper2Value(val matrixIndex: Integer, val rowIndex: Integer, val colValue: Double)

class ReducerKey(val matrixIndex: Integer, val rowIndex: Integer)
class ReducerValue(val matrixIndex: Integer, val colIndex: Integer, val colValue: Double)

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
          new ObjectWritable(new Mapper2Value(k.matrixIndex, k.rowIndex, v.get()))
        )

        context.write(
          new ObjectWritable(new Mapper2Key(k.matrixIndex, i, true)),
          new ObjectWritable(new Mapper2Value(k.matrixIndex, k.rowIndex, v.get()))
        )

      }

    }
  }

  class MatrixNormReducer extends Reducer[ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable] {

    private var min = Double.MaxValue
    private var max = Double.MinPositiveValue

    override def reduce(key: ObjectWritable, values: lang.Iterable[ObjectWritable], context: Reducer[ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable]#Context): Unit = {

      val i$ = values.iterator

      val k = key.get() match { case j: Mapper2Key => j}

      if(!k.flag) {

        while ( {i$.hasNext}) {
          val v = i$.next
          val value = v.get() match { case j: Mapper2Value => j}
          if(value.colValue > max) {
            max = value.colValue
          }
          if(value.colValue < min) {
            min = value.colValue
          }
        }

      } else {

        while ( {i$.hasNext}) {
          val v = i$.next
          val value = v.get() match { case j: Mapper2Value => j }
          val newValue = (value.colValue - min) / (max - min)

          context.write(
            new ObjectWritable(new ReducerKey(k.matrixIndex, value.rowIndex)),
            new ObjectWritable(new ReducerValue(k.matrixIndex, k.colIndex, newValue))
          )

        }

      }

    }
  }

  class MatrixNormComposer extends Reducer[ObjectWritable, ObjectWritable, IntWritable, Text] {
    override def reduce(key: ObjectWritable, values: lang.Iterable[ObjectWritable], context: Reducer[ObjectWritable, ObjectWritable, IntWritable, Text]#Context): Unit = {

      val k = key.get() match { case j:ReducerKey => j}
      context.write(new IntWritable(k.matrixIndex), new Text("ToDo: serialized matrix"))

    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"matrix normalization")
    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[MatrixNormMapper1])
    job.setMapperClass(classOf[MatrixNormMapper2])
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



