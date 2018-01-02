package org.unipi.matrixpnorm;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

public class Utils {

    public static Double[] deserializeArrayOfDoubles(String s) throws IllegalArgumentException {
        String[] items = s.split("\t");

        return Arrays.stream(items).map(Double::valueOf).toArray(Double[]::new);
    }

    public static String serializeArrayOfDoubles(Double[] row) {

        int cols = row.length;

        DecimalFormat format = (DecimalFormat) NumberFormat.getNumberInstance(
                new Locale("en", "UK")
        );
        format.applyPattern("#.####");

        StringBuilder b = new StringBuilder();

        for (int c = 0; c < cols; c++) {
            if (c > 0)
                b.append("\t");
            b.append(format.format(row[c]));
        }

        return b.toString();
    }

}