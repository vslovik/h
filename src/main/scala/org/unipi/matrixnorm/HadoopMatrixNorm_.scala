package org.unipi.matrixnorm

import java.io.IOException
import java.lang
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text, NullWritable, IntWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.unipi.matrixgen.MatrixGenerator
import scala.collection.JavaConverters._
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configured

object HadoopMatrixNorm_ extends Configured with Tool {

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

  class MatrixNormReducer extends Reducer[MapperKey, MapperValue, NullWritable, String] {

    private var min = Double.MaxValue
    private var max = Double.MinPositiveValue

    private var currentMatrixIndex = 0
    private var treeMap = new java.util.TreeMap[Int, Array[Double]]

    def setMinMax(values: lang.Iterable[MapperValue]):Unit = {
      val i$ = values.iterator
      while ( {i$.hasNext}) {
        val value = i$.next match { case j: MapperValue => j}
        if(value.colValue > max) {
          max = value.colValue
        }
        if(value.colValue < min) {
          min = value.colValue
        }
      }
    }

    def keepColumn(values: lang.Iterable[MapperValue]):Array[Double] = {
      var colMap = new java.util.TreeMap[Int, Double]
      val i$ = values.iterator
      while ({i$.hasNext}) {
        val value = i$.next match { case j: MapperValue => j }
        val newValue = (value.colValue - min) / (max - min)
//        val newValue = value.colValue
        colMap.put(value.rowIndex,  newValue)
      }
      colMap.values().asScala.toArray
    }

    def treeMapTo2DList():List[List[Double]] = {
      val rows = treeMap.ceilingEntry(treeMap.ceilingKey(0)).getValue.length
      val cols = treeMap.size()
      val matrix = Array.ofDim[Double](rows, cols)
      for((col, c) <- treeMap.values().asScala.toArray.zipWithIndex) {
        for ((colValue, r) <- col.zipWithIndex) {
          matrix(r)(c) = colValue
        }
      }
      matrix.map(_.toList).toList
    }

    def emitMatrix(context: Reducer[MapperKey, MapperValue, NullWritable, String]#Context):Unit = {
      context.write(
        NullWritable.get(),
        (new MatrixGenerator).serialize(treeMapTo2DList())
      )
    }

    override def reduce(key: MapperKey, values: lang.Iterable[MapperValue],
                        context: Reducer[MapperKey, MapperValue, NullWritable, String]#Context): Unit = {

      if (key.flag == 0) {
        setMinMax(values)
      } else {
        treeMap.put(key.colIndex, keepColumn(values))
        min = Double.MaxValue
        max = Double.MinPositiveValue
      }

      if (key.matrixIndex != currentMatrixIndex) {
        emitMatrix(context)
        currentMatrixIndex = key.matrixIndex
        treeMap = new java.util.TreeMap[Int, Array[Double]]
      }
    }

    @throws[IOException]
    @throws[InterruptedException]
    override  def cleanup(context: Reducer[MapperKey, MapperValue, NullWritable, String]#Context): Unit = {
      emitMatrix(context)
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val res = ToolRunner.run(new Configuration(), HadoopMatrixNorm_, args)
    System.exit(res)
  }

  @throws[Exception]
  def run(args: Array[String]): Int = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration,"matrix normalization")
    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[MatrixNormMapper])

    job.setMapOutputKeyClass(classOf[MapperKey])
    job.setMapOutputValueClass(classOf[MapperValue])

    job.setPartitionerClass(classOf[ColumnIndexPartitioner])
    job.setReducerClass(classOf[MatrixNormReducer])

    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[String])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0
    else 1
  }

}