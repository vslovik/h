package org.unipi.matrixgen;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

class MatrixGenInputFormat extends FileInputFormat<Text, NullWritable> {

    private static final String NUM_MAP_TASKS = "random.generator.map.tasks";
    static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";

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

    static void setNumRecordsPerTask(Job job, int i) {
        job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
    }
}