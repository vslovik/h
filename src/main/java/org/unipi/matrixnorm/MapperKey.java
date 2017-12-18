package org.unipi.matrixnorm;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

public class MapperKey implements WritableComparable<MapperKey> {

    public int matrixIndex = 0;
    public int colIndex    = 0;
    public int flag        = 0;

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

}
