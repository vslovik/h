package org.unipi.matrixgen;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FakeInputSplit extends InputSplit implements Writable {

    public void readFields(DataInput arg0) throws IOException {
    }

    public void write(DataOutput arg0) throws IOException {
    }

    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    public String[] getLocations() throws IOException,
            InterruptedException {
        return new String[0];
    }
}
