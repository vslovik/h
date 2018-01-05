package org.unipi.matrixpnorm;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

class MagnitudesLoader {

    static List<Double> load(String path, Configuration conf) throws IOException
    {
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

        List<Double> magnitudes = new ArrayList<>();

        try {
            String line;
            line = br.readLine();
            while (line != null){

                String[] row = line.split("\t");
                magnitudes.add(Double.parseDouble(row[1]));

                line = br.readLine();
            }
        } finally {

            br.close();
        }

        return magnitudes;
    }
}
