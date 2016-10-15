package com.pbueno.tutos.spark;

import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Pedro on 15/10/2016.
 */
public class JavaStatusTrackerDemo {

    // Dummy class that just adds a 2 seconds delay implementing Spark Function
    public static final class IdentityWithDelay<T> implements Function<T,T> {
        public T call (T x){
            try{
                Thread.sleep(2000L);
            }catch (InterruptedException ex){
                System.out.println ("Received an interruption");
            }
            return x;
        }
    }

    public static void main (String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().master("local").appName("Status tracker").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4,5), 5).map(new IdentityWithDelay<Integer>());

        JavaFutureAction<List<Integer>> jobList = rdd.collectAsync();

        while (!jobList.isDone()){
            Thread.sleep(1000);
            List<Integer> jobIds = jobList.jobIds();
            if (jobIds.isEmpty())
                continue;
            int currentJobId = jobIds.get(jobIds.size()-1);
            SparkJobInfo jobInfo = jsc.statusTracker().getJobInfo(currentJobId);
            SparkStageInfo stageInfo = jsc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
            System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
                    " active, " + stageInfo.numCompletedTasks() + " complete");
        }

        System.out.println("All jobs completed, info is " + jobList.get());
        spark.stop();
    }
}
