package org.unipi.matrixrowgen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class MatrixRowGenDriver extends Configured implements Tool {

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatrixRowGenDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("USAGE : [number of map tasks] [number of matrix rows per task] [number of matrix columns] [output path]");
            return 1;
        } else {

            Configuration configuration = new Configuration();

            int numMapTasks = Integer.parseInt(args[0]);
            int numMatrixRowsPerTasks = Integer.parseInt(args[1]);
            int numMatrixCols = Integer.parseInt(args[2]);
            Path outputDir = new Path(args[3]);

            if(0 == numMapTasks || 0 == numMatrixRowsPerTasks || 0 == numMatrixCols) {
                System.err.println("USAGE : [number of map tasks] [number of matrix rows per task] [number of matrix columns] [output path]");
                return 1;
            }

            Job job = Job.getInstance(configuration, "matrix generator");

            FileSystem.get(outputDir.toUri(), configuration).delete(outputDir, true);

            job.setJarByClass(MatrixRowGenDriver.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(MatrixRowGenInputFormat.class);

            job.setInputFormatClass(MatrixRowGenInputFormat.class);

            MatrixRowGenInputFormat.setNumMapTasks(job, numMapTasks);
            MatrixRowGenInputFormat.setNumMatrixRowsPerTask(job, numMatrixRowsPerTasks);
            MatrixRowGenInputFormat.setNumMatrixCols(job, numMatrixCols);

            FileOutputFormat.setOutputPath(job, outputDir);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            if (job.waitForCompletion(true)) return 0;
            else return 1;
        }
    }
}