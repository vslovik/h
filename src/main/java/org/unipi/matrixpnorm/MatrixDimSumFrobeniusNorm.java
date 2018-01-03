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
import java.nio.file.Files;
import java.nio.file.Paths;

import java.io.IOException;
import java.util.Random;

import static java.lang.Math.pow;
import static java.lang.Math.log;
import static java.lang.Math.min;

public class MatrixDimSumFrobeniusNorm extends Configured implements Tool {

    public class MatrixDimSumFrobeniusNormMapper extends Mapper<Integer, Text, Integer, Double> {

        private Double[] magnitudes;
        private double gamma;
        private Random random = new Random();

        private void init(String magnitudesSerialized) {
            if (null == magnitudes) {
                magnitudes = Utils.deserializeArrayOfDoubles(magnitudesSerialized);
                gamma = 2 * log(magnitudes.length);
            }
        }

        @Override
        public void map(Integer key, Text value, Context context) throws IOException, InterruptedException {
            try {

                double prob;

                init(context.getConfiguration().get("magnitudes_serialized"));

                Double[] row = Utils.deserializeArrayOfDoubles(value.toString());
                if (row.length != magnitudes.length) {
                    throw new IllegalArgumentException();
                }

                for (int c = 0; c < row.length; c++) {
                    if(!row[c].equals(0.0)) {
                        double factor = gamma / magnitudes[c];
                        prob = min(1.0, factor);
                        if (random.nextDouble() < prob) {
                            context.write(c, row[c] * row[c]);
                        }
                    }
                }

            } catch (IllegalArgumentException e) {
                throw new IOException();
            }
        }
    }

    public class MatrixDimSumFrobeniusNormReducer extends Reducer<Integer, Double, NullWritable, Double> {

        Double trace = 0.0;
        private Double[] magnitudes;
        private double gamma;

        private void init(String magnitudesSerialized) {
            if (null == magnitudes) {
                magnitudes = Utils.deserializeArrayOfDoubles(magnitudesSerialized);
                gamma = 2 * log(magnitudes.length);
            }
        }

        @Override
        public void reduce(Integer key, Iterable<Double> values,
                           Context context) throws IOException, InterruptedException {

            init(context.getConfiguration().get("magnitudes_serialized"));

            double sum = 0.0;
            for (Double value : values) {
                sum += value;
            }

            double factor = 1.0 / magnitudes[key];

            if (gamma * factor > 1) {
                trace += factor * sum;
            } else {
                trace += sum / gamma;
            }
        }

        @Override
        public void cleanup(Reducer<Integer, Double, NullWritable, Double>.Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), pow(trace, 0.5));
        }
    }

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopMatrixNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("USAGE : [magnitudes input path] [matrix input path] [output path]");
            return 1;
        } else {
            Configuration conf = new Configuration();

            conf.set("magnitudes_serialized", new String(Files.readAllBytes(Paths.get(args[0]))));

            Job job = Job.getInstance(conf, "matrix dimsum Frobenius norm");
            job.setJarByClass(MatrixDimSumFrobeniusNorm.class);

            job.setMapperClass(MatrixDimSumFrobeniusNormMapper.class);

            job.setMapOutputKeyClass(Integer.class);
            job.setMapOutputValueClass(Double.class);
            job.setReducerClass(MatrixDimSumFrobeniusNormReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(String.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            if (job.waitForCompletion(true)) return 0;
            else return 1;
        }
    }
}
