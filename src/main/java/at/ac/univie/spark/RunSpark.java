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
import org.apache.spark.sql.Row;

import scala.Tuple2;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;


class RunSpark{

	public static void main(String [] args)
	{
		// on AWS:
		// SparkConf conf = new SparkConf().setAppName("GRUPPE01");
//		String AWS_ACCESS_KEY_ID = "";
//		String AWS_SECRET_ACCESS_KEY = "";

		// local environment (laptop/PC)
		//spark.driver.cores -> Number of cores to use for the driver proces, only in cluster mode
		SparkConf conf = new SparkConf().setAppName("GRUPPE01").setMaster("local[*]").set("spark.executor.memory", "6g").set("spark.driver.memory", "2g");
		
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
		String fullPath =  path+"//src//main//resources//kddcup.data_10_percent_corrected.fin";
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
		KMeans kmeans = new KMeans();
		final KMeansModel clusters = kmeans.run(parsedData.rdd());
		System.out.println("Cluster centers:");
		for(Vector center:clusters.clusterCenters()){
			System.out.println("Cluster: " + center);
		}
		final Vector[] centers=clusters.clusterCenters();
		
		JavaPairRDD<Integer, Double> clusterDistance = parsedData.mapToPair(
		        new PairFunction<Vector, Integer, Double> () {
		          public Tuple2<Integer, Double> call(Vector vector) throws Exception{
		            int cluster = clusters.predict(vector);
		            double dist = Math.sqrt(Vectors.sqdist(vector,centers[cluster]));
		            return new Tuple2<Integer, Double>(cluster, dist);
		          }
		        }
		    );
		
		JavaPairRDD<Integer,Double> distance = clusterDistance.reduceByKey((a,b) -> a+b);
		List<Tuple2<Integer,Double>> listDist = distance.collect();
		for(Tuple2<Integer,Double> tuple : listDist){
			System.out.println(">>>>>>>>>>>>>>> Sum distance per Center: " + tuple.toString() + "<<<<<<<<<<<<<<");
		}
		
		JavaPairRDD<Integer, Integer> clusterCount = parsedData.mapToPair(
		        new PairFunction<Vector, Integer, Integer> () {
		          public Tuple2<Integer, Integer> call(Vector vector) throws Exception{
		            return new Tuple2<Integer, Integer>(clusters.predict(vector), 1);
		          }
		        }      
		    );
		    
		    JavaPairRDD<Integer, Integer> count = clusterCount.reduceByKey((a, b) -> a + b);
		    
		    List<Tuple2<Integer,Integer>> idx = count.collect();
		    for(Tuple2<Integer,Integer> tuple : idx){
		    	System.out.println(">>>>>>>>>>>>>>> Elements per Center: " + tuple.toString() + "<<<<<<<<<<<<<<");
		    	
		    }
		    
		    //JavaPairRDD<Integer,Double> rdd = sc.parallelizePairs(listDist);
		    JavaRDD<Tuple2<Integer, Double>> avrg = distance.join(count).map(new Function<Tuple2<Integer,Tuple2<Double,Integer>>, Tuple2<Integer,Double>>() {

				@Override
				public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> v1) throws Exception {
					double avrg = v1._2._1/v1._2._2;
					return new Tuple2<Integer,Double>(v1._1,avrg);
				}
		    	
			});
		    
		    List<Tuple2<Integer, Double>> listAvrg = avrg.collect();
		    for(Tuple2<Integer, Double> tuple : listAvrg){
				System.out.println(">>>>>>>>>>>>>>> AverageList: " + tuple.toString() + "<<<<<<<<<<<<<<");
			}
		sc.close();
		
		
	}
	
	

}
