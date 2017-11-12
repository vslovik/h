package org.unipi.matrixnorm;

import java.util.List;

public class ReduceInputFormat{

    public Tuple<Integer, Boolean> key;
    public List<Tuple<Integer, Double>> value;

    public ReduceInputFormat(Tuple<Integer, Boolean> key, List<Tuple<Integer, Double>> value){
        this.key = key;
        this.value = value;
    }
}
