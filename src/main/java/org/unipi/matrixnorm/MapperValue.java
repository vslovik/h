package org.unipi.matrixnorm;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Arrays;

public class MapperValue implements Writable {

    int rowIndex    = 0;
    double colValue = 0;

    public MapperValue(int rowIndex, double colValue) {
        this.rowIndex = rowIndex;
        this.colValue = colValue;
    }

    public void readFields(DataInput in) throws IOException {

        Double[] arr = Arrays.stream(
                in.readLine().split("\t")
        ).map(String::trim).toArray(Double[]::new);

        this.rowIndex = arr[0].intValue();
        this.colValue = arr[1];
    }

    public void write(DataOutput out) throws IOException {
        double[] array = new double[] {this.rowIndex, this.colValue};
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
        result = prime * result + rowIndex;

        long l = Double.doubleToLongBits(colValue);

        result = prime * result + (int)(l ^ (l >>> 32));
        return result;
    }
}
