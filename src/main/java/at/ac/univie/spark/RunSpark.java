package at.ac.univie.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;


class RunSpark{

	public static void main(String [] args)
	{
		// on AWS:
		// SparkConf conf = new SparkConf().setAppName("GRUPPEXX");

		// local environment (laptop/PC)
		SparkConf conf = new SparkConf().setAppName("GRUPPEXX").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		String AWS_ACCESS_KEY_ID = "";
		String AWS_SECRET_ACCESS_KEY = "";

		System.out.println(">>>>>>>>>>>>>>>>>> Hello from Spark! <<<<<<<<<<<<<<<<<<<<<<<<<");
		String path = System.getProperty("user.dir");

//  ...example accessing S3:


//    clusterMembers.saveAsTextFile("s3n://" + AWS_ACCESS_KEY_ID + ":" + AWS_SECRET_ACCESS_KEY + "@qltrail-lab-265-1488270472/result");
		
		//1.3 Count Labels
		JavaRDD<String> textFile = sc.textFile(path+"//src//main//resources//kddcup.data.label.corrected");
		JavaPairRDD<String, Integer> counts = textFile
		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		    .mapToPair(word -> new Tuple2<>(word, 1))
		    .reduceByKey((a, b) -> a + b);
		counts.coalesce(1).saveAsTextFile(path+"//src//main//resources//LabelCount//");

		sc.stop();
		
		
	}

}
