package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.unipi.matrixnorm.HadoopMatrixNorm;

import java.io.IOException;

import static java.lang.Math.pow;

public class MatrixNaiveFrobeniusNorm extends Configured implements Tool {

    public static class MatrixNaiveFrobeniusNormMapper extends Mapper<Integer, Text, Integer, Double> {

        @Override
        public void map(Integer key, Text value, Context context) throws IOException, InterruptedException {
            Double[] row = Utils.deserializeArrayOfDoubles(value.toString());
            for (int c = 0; c < row.length; c++) {
                if (!row[c].equals(0.0)) {
                    context.write(c, row[c] * row[c]);
                }
            }
        }
    }

    public static class MatrixNaiveFrobeniusNormReducer extends Reducer<Integer, Double, NullWritable, Double> {

        Double trace = 0.0;

        @Override
        public void reduce(Integer key, Iterable<Double> values,
                           Context context) throws IOException, InterruptedException {
            for (Double value : values) {
                trace += value;
            }
        }

        @Override
        public void cleanup(Reducer<Integer, Double, NullWritable, Double>.Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), pow(trace, 0.5));
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatrixNaiveFrobeniusNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("USAGE : [input path] [output path]");
            return 1;
        } else {
            Configuration conf = new Configuration();
            conf.setDouble("power", 2.0);
            Job job = Job.getInstance(conf, "matrix naive Frobenius norm");

            job.setJarByClass(MatrixNaiveFrobeniusNorm.class);

            job.setMapperClass(MatrixNaiveFrobeniusNormMapper.class);
            job.setMapOutputKeyClass(Integer.class);
            job.setMapOutputValueClass(Double.class);

            job.setReducerClass(MatrixNaiveFrobeniusNormReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Double.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if (job.waitForCompletion(true)) return 0;
            else return 1;
        }
    }
}
