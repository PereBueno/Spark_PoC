package com.pbueno.tutos.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount{
    private static final Pattern SPACE = Pattern.compile(" ");
    private static String fileToProcess = "src/main/resources/input.txt";

    public static void main(String[] args) throws Exception{
        System.out.println("Testing");

        if (args.length >= 1){
            fileToProcess = args[1];
        }

        //TODO: This line sets master as local instead of a real cluster, just for dev purposes
        SparkSession spark = SparkSession.builder().master("local[5]").appName("WordCount").getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(fileToProcess).javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
            //@Override
            public Iterator<String> call(String s){
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        );

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?,?> tuple : output){
            System.out.println(tuple._1() + " - " + tuple._2());
        }

        while (true){
            try {
                Thread.sleep(5000L);
            }catch(InterruptedException ex){
                spark.stop();
            }
        }

    }
}