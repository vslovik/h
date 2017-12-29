package org.unipi.matrixnorm;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;

public class MapperKey implements WritableComparable<MapperKey> {

    int matrixIndex = 0;
    int colIndex    = 0;
    int flag        = 0;

    public MapperKey(int matrixIndex, int colIndex, int flag) {
        this.matrixIndex = matrixIndex;
        this.colIndex    = colIndex;
        this.flag        = flag;
    }

    public void readFields(DataInput in) throws IOException {

        Integer[] arr = Arrays.stream(
                in.readLine().split("\t")
        ).map(String::trim).toArray(Integer[]::new);

        this.matrixIndex = arr[0];
        this.colIndex    = arr[1];
        this.flag        = arr[2];
    }

    public void write(DataOutput out) throws IOException {
        Integer[] array = new Integer[] {this.matrixIndex, this.colIndex, this.flag};
        out.writeBytes(Arrays.toString(array));
    }

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
