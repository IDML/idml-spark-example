package org.idml.sparkexample;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import org.idml.spark.IDMLMap;
import com.google.gson.Gson;

public final class IDMLExample {

public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
		    System.err.println("Usage: IDMLExample <IDML mapping file> <JSON text file>");
		    System.exit(1);
	    }
		
		SparkConf sparkConf = new SparkConf().setAppName("IDMLExample");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
	    JavaRDD<String> lines = ctx.textFile(args[1], 1);
	    
	    IDMLMap map = new IDMLMap(args[0]);
	    JavaRDD<String> results = lines.map(map);
	   
		// Perform some kind of map reduce
	    JavaPairRDD<String, Integer> locations = results.mapToPair(new PairFunction<String, String, Integer>() {
	    	public Tuple2<String, Integer> call(String s) 
	    	{ 
	    		Map interaction = new Gson().fromJson(s, Map.class);
	    		
	    		if(interaction.containsKey("location"))
	    		{
	    			return new Tuple2<String, Integer>(interaction.get("location").toString(), 1);
	    		}
	    		else
	    		{
	    			return null;
	    		}
	    	}
    	});
	    
	    locations = locations.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> val) throws Exception {
				if(val == null) 
					return false;
				else
					return true;
			}
	    });
	    
	    JavaPairRDD<String, Integer> locationCounts = locations.reduceByKey(new Function2<Integer, Integer, Integer>() {
	     	public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
	    });
	    
		List<Tuple2<String, Integer>> output = locationCounts.collect();
		
	    for (Tuple2<String, Integer> result : output) {
	      System.out.println("RESULT: " + result._1 + " " + result._2);
	    }
	    
	    ctx.stop();
		
	}

}
