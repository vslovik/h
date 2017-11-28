package org.unipi.matrixnorm

import java.io.{DataInput, DataOutput, IOException}
import java.lang

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Writable, WritableComparable, LongWritable, ObjectWritable, Text, IntWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.unipi.matrixgen.MatrixGenerator
import util.control.Exception._

import scala.collection.JavaConverters._

class RowKey(var matrixIndex: Integer, var rowIndex: Integer) extends WritableComparable[RowKey] {

  def this() = this(matrixIndex = 0, rowIndex = 0)

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    val arr = in.readLine().split(" ").map(_.trim)
    this.matrixIndex = arr(0).toInt
    this.rowIndex = arr(1).toInt
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.rowIndex.toString).mkString(" ")
    out.writeBytes(data)
  }

  override def compareTo(o: RowKey): Int = {
    val thisSeq = Seq[Int](this.matrixIndex, this.rowIndex)
    val thatSeq = Seq[Int](o.matrixIndex, o.rowIndex)
    Utils.compare(thisSeq, thatSeq)
  }
}
class PartitionerKey(val matrixIndex: Integer, val colIndex: Integer)
class MapperKey(var matrixIndex: Integer, var colIndex: Integer, var flag: Integer) extends WritableComparable[MapperKey] {

  def this() = this(matrixIndex = 0, colIndex = 0, flag = 0)

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    val arr = in.readLine().split(" ").map(_.trim)
    this.matrixIndex = arr(0).toInt
    this.colIndex = arr(1).toInt
    this.flag = arr(2).toInt
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.colIndex.toString, this.flag.toString).mkString(" ")
    out.writeBytes(data)
  }

  override def compareTo(o: MapperKey): Int = {
    val thisSeq = Seq[Int](this.matrixIndex, this.colIndex, this.flag)
    val thatSeq = Seq[Int](o.matrixIndex, o.colIndex, o.flag)
    Utils.compare(thisSeq, thatSeq)
  }
}

class MapperValue(var matrixIndex: Integer, var rowIndex: Integer, var colValue: Double) extends Writable {

  def this() = this(matrixIndex = 0, rowIndex = 0, colValue = 0.0)

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    val arr = in.readLine().split(" ").map(_.trim)
    this.matrixIndex = BigInt(arr(0)).intValue()
    this.rowIndex = BigInt(arr(1)).intValue()
    this.colValue = BigDecimal(arr(2)).doubleValue()
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.rowIndex.toString, this.colValue.toString).mkString("\t")
    out.writeBytes(data)
  }
}

class ReducerValue(var matrixIndex: Integer, var colIndex: Integer, var colValue: Double) extends Writable {
  def this() = this(matrixIndex = 0, colIndex = 0, colValue = 0.0)

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    val arr = in.readLine().split(" ").map(_.trim)
    this.matrixIndex = BigInt(arr(0)).intValue()
    this.colIndex = BigInt(arr(1)).intValue()
    this.colValue = BigDecimal(arr(2)).doubleValue()
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.colIndex.toString, this.colValue.toString).mkString("\t")
    out.writeBytes(data)
  }
}

object HadoopMatrixNorm {

  class MatrixNormMapper extends Mapper[LongWritable, Text, MapperKey, MapperValue] {

    private var matrixIndex = 0

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MapperKey, MapperValue]#Context): Unit = {
      val matrix = (new MatrixGenerator).deserialize(value.toString)
      for (rowIndex <- matrix.indices) {
        for (columnIndex <- matrix(rowIndex).indices) {
          context.write(
            new MapperKey(matrixIndex, columnIndex, 0),
            new MapperValue(matrixIndex, rowIndex, matrix(rowIndex)(columnIndex))
          )
          context.write(
            new MapperKey(matrixIndex, columnIndex, 1),
            new MapperValue(matrixIndex, rowIndex, matrix(rowIndex)(columnIndex))
          )
        }
      }
      matrixIndex += 1
    }
  }

  class ColumnIndexPartitioner extends HashPartitioner[MapperKey, MapperValue] {
    override def getPartition(key: MapperKey, value: MapperValue, numReduceTasks: Int): Int = {
      super.getPartition(new MapperKey(key.matrixIndex, key.colIndex, 0), value, numReduceTasks)
    }
  }

  class MatrixNormReducer extends Reducer[MapperKey, MapperValue, RowKey, ReducerValue] {

    private var min = Double.MaxValue
    private var max = Double.MinPositiveValue

    override def reduce(key: MapperKey, values: lang.Iterable[MapperValue], context: Reducer[MapperKey, MapperValue, RowKey, ReducerValue]#Context): Unit = {
      val i$ = values.iterator
      if(key.flag == 0) {
        while ( {i$.hasNext}) {
          val value = i$.next match { case j: MapperValue => j}
          if(value.colValue > max) {
            max = value.colValue
          }
          if(value.colValue < min) {
            min = value.colValue
          }
        }
      } else {
        while ( {i$.hasNext}) {
          val value = i$.next match { case j: MapperValue => j }
          val newValue = (value.colValue - min) / (max - min)
          context.write(
            new RowKey(key.matrixIndex, value.rowIndex),
            new ReducerValue(key.matrixIndex, key.colIndex, newValue)
          )
        }
      }
    }
  }

  class MatrixNormComposer extends Reducer[RowKey, ReducerValue, IntWritable, String] {

    private var currentMatrixIndex = 0
    private var matrix = new java.util.TreeMap[Int, Array[Double]]
    private val mg = new MatrixGenerator

    override def reduce(key: RowKey, values: lang.Iterable[ReducerValue], context: Reducer[RowKey, ReducerValue, IntWritable, String]#Context): Unit = {
      val row: Array[Double] = values.iterator.asScala.toArray.map { v => v.colValue }
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
    job.setMapOutputValueClass(classOf[MapperValue])

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



