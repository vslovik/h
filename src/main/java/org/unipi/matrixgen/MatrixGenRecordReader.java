package org.unipi.matrixgen;

import org.unipi.matrixnorm.Utils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Random;

public class MatrixGenRecordReader extends RecordReader<Text, NullWritable> {

    private int numRecordsToCreate = 100;
    private int createdRecords = 0;

    private Text key = new Text();
    private NullWritable value = NullWritable.get();

    private Random r = new Random();

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.numRecordsToCreate = context.getConfiguration().getInt(MatrixGenInputFormat.NUM_RECORDS_PER_TASK, -1);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        int minRows = 1;
        int minCols = 1;

        int maxRows = 50;
        int maxCols = 50;

        int minValue = 1;
        int maxValue = 100;

        if (createdRecords < numRecordsToCreate) {
            int rows = minRows + r.nextInt(maxRows - minRows);
            int cols = minCols + r.nextInt(maxCols - minCols);
            int limit = minValue + r.nextInt(maxValue - minValue);
            String serializedMatrix = Utils.serialize(Utils.generateMatrix(limit, rows, cols));
            key.set(serializedMatrix);
            createdRecords += 1;

            return true;
        }

        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return (float) createdRecords / (float) numRecordsToCreate;
    }

    public void close() throws IOException {
    }
}