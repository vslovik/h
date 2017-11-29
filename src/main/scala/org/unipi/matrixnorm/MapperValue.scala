package org.unipi.matrixnorm

import java.io.{DataInput, DataOutput, IOException}
import org.apache.hadoop.io.Writable

class MapperValue(var matrixIndex: Integer, var rowIndex: Integer, var colValue: Double) extends Writable {

  def this() = this(matrixIndex = 0, rowIndex = 0, colValue = 0.0)

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    val arr = in.readLine().split("\t").map(_.trim)
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