package org.unipi.matrixnorm

import java.io.{IOException, DataInput, DataOutput}
import java.lang
import java.lang.Comparable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.{IntWritable, ObjectWritable, WritableComparable, WritableComparator}
import org.unipi.matrixgen.MatrixGenerator

import scala.collection.JavaConverters._

class RowKey(var matrixIndex: Integer, var rowIndex: Integer) extends WritableComparable[RowKey] {

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    this.matrixIndex = in.readInt
    this.rowIndex = in.readInt
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.rowIndex.toString).mkString(" ")
    out.writeBytes(data)
  }

  override def compareTo(o: RowKey): Int = {
    if (this.matrixIndex < o.matrixIndex) -1
    else if (this.matrixIndex == o.matrixIndex) {

      if (this.rowIndex < o.rowIndex)-1
      else if (this.rowIndex == o.rowIndex) 0
      else 1

    }
    else 1
  }
}
class PartitionerKey(val matrixIndex: Integer, val colIndex: Integer)
class MapperKey(var matrixIndex: Integer, var colIndex: Integer, var flag: Boolean) extends WritableComparable[MapperKey] {

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    this.matrixIndex = in.readInt
    this.colIndex = in.readInt
    this.flag = in.readBoolean
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.colIndex.toString, this.flag.toString).mkString(" ")
    out.writeBytes(data)
  }

  override def compareTo(o: MapperKey): Int = {
    if (this.matrixIndex < o.matrixIndex) -1
    else if (this.matrixIndex == o.matrixIndex) {

      if (this.colIndex < o.colIndex)-1
      else if (this.colIndex == o.colIndex) {

        if (this.flag < o.flag) -1
        else if (this.flag == o.flag) 0
        else 1

      }
      else 1
    }
    else 1

  }

  private def compare(thisValue: Integer, thatValue: Integer): Integer = {
    if (thisValue < thatValue) -1
    else if (thisValue < thatValue) 0
    else 1
  }

}

class MapperValue(val matrixIndex: Integer, val rowIndex: Integer, val colValue: Double)

class ReducerValue(val matrixIndex: Integer, val colIndex: Integer, val colValue: Double)
class RowComposerValue(val matrixIndex: Integer, val row: Array[Double])

object HadoopMatrixNorm {

  class MatrixNormMapper extends Mapper[LongWritable, Text, MapperKey, ObjectWritable] {

    private var matrixIndex = 0

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MapperKey, ObjectWritable]#Context): Unit = {
      val matrix = (new MatrixGenerator).deserialize(value.toString)
      for (rowIndex <- matrix.indices) {
        for (columnIndex <- matrix(rowIndex).indices) {
          context.write(
            new MapperKey(matrixIndex, columnIndex, false),
            new ObjectWritable(new MapperValue(matrixIndex, rowIndex, matrix(rowIndex)(columnIndex)))
          )
          context.write(
            new MapperKey(matrixIndex, columnIndex, true),
            new ObjectWritable(new MapperValue(matrixIndex, rowIndex, matrix(rowIndex)(columnIndex)))
          )
        }
      }
      matrixIndex += 1
    }
  }

  class ColumnIndexPartitioner extends HashPartitioner[MapperKey, ObjectWritable] {
    override def getPartition(key: MapperKey, value: ObjectWritable, numReduceTasks: Int): Int = {
      super.getPartition(new MapperKey(key.matrixIndex, key.colIndex, false), value, numReduceTasks)
    }
  }

  class MatrixNormReducer extends Reducer[MapperKey, ObjectWritable, RowKey, ObjectWritable] {

    private var min = Double.MaxValue
    private var max = Double.MinPositiveValue

    override def reduce(key: MapperKey, values: lang.Iterable[ObjectWritable], context: Reducer[MapperKey, ObjectWritable, RowKey, ObjectWritable]#Context): Unit = {
      val i$ = values.iterator
      if(!key.flag) {
        while ( {i$.hasNext}) {
          val v = i$.next
          val value = v.get() match { case j: MapperValue => j}
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
          val value = v.get() match { case j: MapperValue => j }
          val newValue = (value.colValue - min) / (max - min)
          context.write(
            new RowKey(key.matrixIndex, value.rowIndex),
            new ObjectWritable(new ReducerValue(key.matrixIndex, key.colIndex, newValue))
          )
        }
      }
    }
  }

  class MatrixNormComposer extends Reducer[RowKey, ObjectWritable, IntWritable, String] {

    private var currentMatrixIndex = 0
    private var matrix = new java.util.TreeMap[Int, Array[Double]]
    private val mg = new MatrixGenerator

    override def reduce(key: RowKey, values: lang.Iterable[ObjectWritable], context: Reducer[RowKey, ObjectWritable, IntWritable, String]#Context): Unit = {
      val row: Array[Double] = values.iterator.asScala.toArray.map { v => v.get() match { case j: ReducerValue => j.colValue }}
      if (key.matrixIndex != currentMatrixIndex) {
        context.write(new IntWritable(currentMatrixIndex), mg.serialize(matrix.values.toArray))
        matrix = new java.util.TreeMap[Int, Array[Double]]
      }
      matrix.put(key.rowIndex, row)
    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"matrix normalization")
    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[MatrixNormMapper])

    job.setMapOutputKeyClass(classOf[MapperKey])
    job.setMapOutputValueClass(classOf[ObjectWritable])

    job.setPartitionerClass(classOf[ColumnIndexPartitioner])

    job.setReducerClass(classOf[MatrixNormReducer])
    job.setReducerClass(classOf[MatrixNormComposer])

    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[String])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}



