package org.unipi.matrixpnorm;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

class MagnitudesLoader {

    static List<Double> load(String path, Configuration conf) throws IOException
    {
        String line;
        List<Double> magnitudes = new ArrayList<>();

        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)))) {
            line = br.readLine();
            while (line != null){

                String[] row = line.split("\\s+");
                magnitudes.add(Double.parseDouble(row[1]));

                line = br.readLine();
            }
        }

        return magnitudes;
    }
}
