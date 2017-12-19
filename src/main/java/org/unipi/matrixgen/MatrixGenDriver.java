package org.unipi.matrixgen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

class MatrixGenDriver extends Configured implements Tool {

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatrixGenDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("USAGE : [number of map tasks] [number of records per task] [output path]");
            return 1;
        } else {

            Configuration configuration = new Configuration();

            int numMapTasks = Integer.parseInt(args[0]);
            int numRecordsPerTasks = Integer.parseInt(args[1]);
            Path outputDir = new Path(args[2]);

            Job job = Job.getInstance(configuration, "matrix generator");

            FileSystem.get(outputDir.toUri(), configuration).delete(outputDir, true);

            job.setJarByClass(MatrixGenDriver.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(MatrixGenInputFormat.class);

            job.setInputFormatClass(MatrixGenInputFormat.class);

            MatrixGenInputFormat.setNumMapTasks(job, numMapTasks);
            MatrixGenInputFormat.setNumRecordsPerTask(job, numRecordsPerTasks);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, outputDir);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            if (job.waitForCompletion(true)) return 0;
            else return 1;
        }
    }
}