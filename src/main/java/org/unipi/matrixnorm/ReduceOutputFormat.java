package org.unipi.matrixnorm;

public class ReduceOutputFormat {

    public Integer key;
    public Tuple<Integer, Double> value;

    public ReduceOutputFormat(Integer key, Tuple<Integer,Double> value){
        this.key = key;
        this.value = value;
    }
}
