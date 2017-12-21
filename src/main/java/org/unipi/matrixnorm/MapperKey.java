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

        String[] arr = Arrays.stream(
                in.readLine().split("\t")
        ).map(String::trim).toArray(String[]::new);

        this.matrixIndex = Integer.parseInt(arr[0]);
        this.colIndex    = Integer.parseInt(arr[1]);
        this.flag        = Integer.parseInt(arr[2]);
    }

    public void write(DataOutput out) throws IOException {
        String[] array = new String[] {Integer.toString(this.matrixIndex), Integer.toString(this.colIndex), Integer.toString(this.flag)};
        out.writeBytes(Arrays.toString(array));
    }

    public int compareTo(MapperKey o) {

        ArrayList<Integer> thisSeq = new ArrayList<>();
        thisSeq.add(this.matrixIndex);
        thisSeq.add(this.colIndex);
        thisSeq.add(this.flag);

        ArrayList<Integer> thatSeq = new ArrayList<>();

        thatSeq.add(o.matrixIndex);
        thatSeq.add(o.colIndex);
        thatSeq.add(o.flag);

        return Utils.compare(thisSeq, thatSeq);
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
