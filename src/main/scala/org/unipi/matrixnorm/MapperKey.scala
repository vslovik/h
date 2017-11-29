package org.unipi.matrixnorm

import java.io.{DataInput, DataOutput, IOException}
import org.apache.hadoop.io.WritableComparable

class MapperKey(var matrixIndex: Integer, var colIndex: Integer, var flag: Integer) extends WritableComparable[MapperKey] {

  def this() = this(matrixIndex = 0, colIndex = 0, flag = 0)

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    val arr = in.readLine().split("\t").map(_.trim)
    this.matrixIndex = BigInt(arr(0)).intValue()
    this.colIndex = BigInt(arr(1)).intValue()
    this.flag = BigInt(arr(2)).intValue()
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    val data = Array(this.matrixIndex.toString, this.colIndex.toString, this.flag.toString).mkString("\t")
    out.writeBytes(data)
  }

  override def compareTo(o: MapperKey): Int = {
    val thisSeq = Seq[Int](this.matrixIndex, this.colIndex, this.flag)
    val thatSeq = Seq[Int](o.matrixIndex, o.colIndex, o.flag)
    Utils.compare(thisSeq, thatSeq)
  }
}