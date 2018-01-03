package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.unipi.matrixnorm.HadoopMatrixNorm;

import java.io.IOException;
import static java.lang.Math.sqrt;
import static java.util.Arrays.fill;

public class MatrixColumnsMagnitudes extends Configured implements Tool {

    public static class MatrixColumnsMagnitudesMapper extends Mapper<Integer, Text, NullWritable, String> {

        private double maxValue = 0.0;
        private Double[] magnitudes;

        @Override
        public void map(Integer key, Text value, Context context) throws IOException, InterruptedException {

            try {

                Double[] row = Utils.deserializeArrayOfDoubles(value.toString());

                if(key.equals(0)) {
                    magnitudes = new Double[row.length];
                    fill(magnitudes, 0.0);
                }

                for(int c = 0; c < row.length; c++) {
                    if(row[c] > maxValue) {
                        maxValue = row[c];
                    }
                    if(!row[c].equals(0.0)) {
                        magnitudes[c] += row[c] * row[c];
                    }
                }

            } catch (IllegalArgumentException e) {
                throw new IOException();
            }
        }

        @Override
        public void cleanup(Mapper<Integer, Text, NullWritable, String>.Context context)
                throws IOException, InterruptedException {

            for(int i = 0; i < magnitudes.length; i++) {
                magnitudes[i] = sqrt(magnitudes[i])/maxValue;
                magnitudes[i] *= magnitudes[i];
            }

            context.write(NullWritable.get(), Utils.serializeArrayOfDoubles(magnitudes));
        }
    }

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopMatrixNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix scale");
        job.setJarByClass(MatrixColumnsMagnitudes.class);
        job.setNumReduceTasks(0);

        job.setMapperClass(MatrixColumnsMagnitudesMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Double.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Integer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }
}