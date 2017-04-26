package at.ac.univie.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

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
		// SparkConf conf = new SparkConf().setAppName("GRUPPE01");
//		String AWS_ACCESS_KEY_ID = "";
//		String AWS_SECRET_ACCESS_KEY = "";

		// local environment (laptop/PC)
		SparkConf conf = new SparkConf().setAppName("GRUPPE01").setMaster("local[*]");
		String path = System.getProperty("user.dir");

		JavaSparkContext sc = new JavaSparkContext(conf);
		System.out.println(">>>>>>>>>>>>>>>>>> Hello from Spark! <<<<<<<<<<<<<<<<<<<<<<<<<");
		

//  ...example accessing S3:


//    clusterMembers.saveAsTextFile("s3n://" + AWS_ACCESS_KEY_ID + ":" + AWS_SECRET_ACCESS_KEY + "@qltrail-lab-265-1488270472/result");
		
		//1.2 List the clustering labels (last column) and their distinct counts
//		JavaRDD<String> textFile = sc.textFile(path+"//src//main//resources//kddcup.data.label.corrected");
//		JavaPairRDD<String, Integer> counts = textFile
//		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//		    .mapToPair(word -> new Tuple2<>(word, 1))
//		    .reduceByKey((a, b) -> a + b);
//		counts.coalesce(1).saveAsTextFile(path+"//src//main//resources//LabelCount//");
//		
		//1.3 Apply K-Means clustering on data with the default settings on parameter
		String fullPath =  path+"//src//main//resources//kddcup.data.test.fin";
		JavaRDD<String> data = sc.textFile(fullPath);
		JavaRDD<Vector> parsedData = data.map(
				new Function<String, Vector>() {
					public Vector call(String s) {
						String[] sarray = s.split(",");
						double[] values = new double[sarray.length];
						for(int i=0; i<sarray.length; i++){
							values[i] = Double.parseDouble(sarray[i]);
						}
						return Vectors.dense(values);
					}
				});
		parsedData.cache();
		// Cluster the data into two classes using KMeans
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
		  System.out.println(" " + center);
		}
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		sc.stop();
		
		
	}

}
