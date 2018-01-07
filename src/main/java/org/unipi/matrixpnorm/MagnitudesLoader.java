package org.unipi.matrixpnorm;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;

class MagnitudesLoader {

    static List<Double> load(String path, Configuration conf) throws IOException
    {
        String line;
        List<Double> magnitudes = new ArrayList<>();

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fileStatus = fs.listStatus(new Path("mag"));
        List<String> parts = new ArrayList<>();
        for(FileStatus status : fileStatus){
            String partPath =  status.getPath().toString();
            String[] str = partPath.split("/");
            if(str[str.length - 1].substring(0,"part".length()).equals("part"))
                parts.add(partPath);
        }

        for(String p : parts) {
            Path pt = new Path(p);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)))) {
                line = br.readLine();
                while (line != null) {

                    String[] row = line.split("\\s+");
                    magnitudes.add(Double.parseDouble(row[1]));

                    line = br.readLine();
                }
            }
        }

        return magnitudes;
    }
}
