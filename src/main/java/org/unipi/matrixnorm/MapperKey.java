package org.unipi.matrixnorm;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapperKey implements WritableComparable<MapperKey> {

    int matrixIndex = 0;
    int colIndex    = 0;
    int flag        = 0;

    public MapperKey(){}

    public MapperKey(int matrixIndex, int colIndex, int flag) {
        this.matrixIndex = matrixIndex;
        this.colIndex    = colIndex;
        this.flag        = flag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        matrixIndex = in.readInt();
        colIndex    = in.readInt();
        flag        = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(matrixIndex);
        out.writeInt(colIndex);
        out.writeInt(flag);
    }

    public String toString() {
        return Integer.toString(matrixIndex) + "\t" + Integer.toString(colIndex) + "\t" + Integer.toString(flag);
    }

    @Override
    public int compareTo(MapperKey o) {

        int result = Integer.compare(this.matrixIndex, o.matrixIndex);
        if (0 == result) {

            result = Integer.compare(this.colIndex, o.colIndex);
            if(0 == result) {

                return Integer.compare(this.flag, o.flag);
            }

            return result;
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        MapperKey other = (MapperKey) o;
        return this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int prime = 37;
        int result = 1;
        result = prime * result + matrixIndex;
        result = prime * result + colIndex;
        result = prime * result + flag;

        return result;
    }

}
