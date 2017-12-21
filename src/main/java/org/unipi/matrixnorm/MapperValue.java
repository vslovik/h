package org.unipi.matrixnorm;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Arrays;

public class MapperValue implements Writable {

    private int matrixIndex = 0;
    int rowIndex    = 0;
    double colValue = 0;

    public MapperValue(int matrixIndex, int rowIndex, double colValue) {
        this.matrixIndex = matrixIndex;
        this.rowIndex    = rowIndex;
        this.colValue    = colValue;
    }

    public void readFields(DataInput in) throws IOException {

        String[] arr = Arrays.stream(
                in.readLine().split("\t")
        ).map(String::trim).toArray(String[]::new);

        this.matrixIndex = Integer.parseInt(arr[0]);
        this.rowIndex    = Integer.parseInt(arr[1]);
        this.colValue    = Double.parseDouble(arr[2]);
    }

    public void write(DataOutput out) throws IOException {
        String[] array = new String[] {Integer.toString(this.matrixIndex), Integer.toString(this.rowIndex), Double.toString(this.colValue)};
        out.writeBytes(Arrays.toString(array));
    }

    public boolean equals(Object o) {
        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        MapperValue other = (MapperValue) o;
        return this.hashCode() == other.hashCode();
    }

    public int hashCode() {
        int prime = 37;
        int result = 1;
        result = prime * result + matrixIndex;
        result = prime * result + rowIndex;

        long l = Double.doubleToLongBits(colValue);

        result = prime * result + (int)(l ^ (l >>> 32));;
        return result;
    }
}
