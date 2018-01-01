package org.unipi.matrixrowgen;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.unipi.matrixgen.FakeInputSplit;
import org.unipi.matrixgen.MatrixGenRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class MatrixRowGenInputFormat extends FileInputFormat<Text, NullWritable> {

    private static final String NUM_MAP_TASKS    = "random.generator.map.tasks";
    static final String NUM_MATRIX_ROWS_PER_TASK = "random.generator.num.matrix.rows.per.map.task";
    static final String NUM_MATRIX_COLS          = "random.generator.num.matrix.cols";

    public List<InputSplit> getSplits(JobContext job) throws IOException {

        int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);

        ArrayList<InputSplit> splits = new ArrayList<>();

        for (int i = 0; i < numSplits; i++) {
            splits.add(new FakeInputSplit());
        }

        return splits;
    }

    public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        MatrixGenRecordReader rr = new MatrixGenRecordReader();
        rr.initialize(split, context);
        return rr;
    }

    static void setNumMapTasks(Job job, int i) {
        job.getConfiguration().setInt(NUM_MAP_TASKS, i);
    }

    static void setNumMatrixRowsPerTask(Job job, int i) {
        job.getConfiguration().setInt(NUM_MATRIX_ROWS_PER_TASK, i);
    }

    static void setNumMatrixCols(Job job, int i) {
        job.getConfiguration().setInt(NUM_MATRIX_COLS, i);
    }
}