package org.unipi.matrixgen;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.unipi.matrixnorm.Utils;
import scala.util.Random;

class MatrixGenRecordReader extends RecordReader<Text, NullWritable> {

    private int numRecordsToCreate = 100;
    private int createdRecords = 0;

    private static int minRows = 1;
    private static int minCols = 1;

    private static int maxRows = 50;
    private static int maxCols = 50;

    private static int minValue = 1;
    private static int maxValue = 100;

    private Text key = new Text();
    private NullWritable value = NullWritable.get();

    private MatrixGenerator mg = new MatrixGenerator();
    private Random r = new Random();

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.numRecordsToCreate = context.getConfiguration().getInt(MatrixGenInputFormat.NUM_RECORDS_PER_TASK, -1);

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (createdRecords < numRecordsToCreate) {
            int rows = minRows + r.nextInt(maxRows - minRows);
            int cols = minCols + r.nextInt(maxCols - minCols);
            int limit = minValue + r.nextInt(maxValue - minValue);
            String serializedMatrix = mg.serialize(mg.generateMatrix(limit, rows, cols)); // ToDo generate string
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