package org.unipi.matrixnorm;

public class MapOutputFormat {

    public Tuple<Integer, Boolean> key;
    public Tuple<Integer, Double> value;

    public MapOutputFormat(Tuple<Integer, Boolean> key, Tuple<Integer, Double> value)
    {
        this.key = key;
        this.value = value;
    }
}
