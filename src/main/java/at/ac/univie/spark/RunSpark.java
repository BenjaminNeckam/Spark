package at.ac.univie.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Arrays;

import java.util.List;

import org.apache.spark.SparkConf;


class RunSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		//String path = System.getProperty("user.dir");
		
		
		if(args[0].equals("local")){
			RunSpark.initLocal(conf);
			if(args[1].equals("label")){
				String path = args[2];
				RunSpark.getLabels(conf, path);
			}else if(args[1].equals("defaultKMeans")){
				String path = args[2];
				RunSpark.kMeansClusteringDefault(conf, path);
			}else if(args[1].equals("KMeans")){
				String path = args[3];
				RunSpark.kMeansClustering(conf, path, Integer.parseInt(args[2]));
			}
		}

	}

	public static void getLabels(SparkConf conf, String path) {
		// 1.2 List the clustering labels (last column) and their distinct
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> textFile = sc.textFile(path + "//src//main//resources//kddcup.data.label.corrected");
		JavaRDD<String> textFile = sc.textFile(path);
		long startTime = System.nanoTime();      
		JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		double estimatedTime = (System.nanoTime() - startTime)/ 1000000000.0;
		List<Tuple2<String, Integer>> lableList = counts.collect();
		System.out.println("\n\n>>>>>>>>>>>>>>>>>>>>>>>>> Clustering Labels <<<<<<<<<<<<<<<<<<<<<<<<<\n");
		for (Tuple2<String, Integer> label : lableList) {
			System.out.println(label.toString());
		}
		System.out.println("\nElapsed Time: " + estimatedTime + " seconds");
		System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>> Clustering Labels <<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
		sc.close();
	}

	public static void kMeansClustering(SparkConf conf, String path, int k) {
		JavaSparkContext sc = new JavaSparkContext(conf);
		LongAccumulator accumSum = sc.sc().longAccumulator();
		LongAccumulator accumElements = sc.sc().longAccumulator();

		long startTime = System.nanoTime();
		//String fullPath = path + "//src//main//resources//kddcup.data_10_percent_corrected.fin";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
			public Vector call(String s) {
				String[] sarray = s.split(",");
				double[] values = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++) {
					values[i] = Double.parseDouble(sarray[i]);
				}
				return Vectors.dense(values);
			}
		});

		parsedData.cache();
		KMeans kmeans = new KMeans();
		kmeans.setK(k);
		kmeans.setMaxIterations(10);
		//kmeans.setRuns(10);
		kmeans.setEpsilon(1.0e-6);
		final KMeansModel clusters = kmeans.run(parsedData.rdd());
		final Vector[] centers = clusters.clusterCenters();

		/**
		 * mapToPair = Distance from data point to nearest Cluster foreach = Sum
		 * of all distances and elements
		 */
		parsedData.mapToPair(new PairFunction<Vector, Integer, Double>() {
			public Tuple2<Integer, Double> call(Vector vector) throws Exception {
				int clusterOfPoint = clusters.predict(vector);
				double dist = Math.sqrt(Vectors.sqdist(vector, centers[clusterOfPoint]));
				return new Tuple2<Integer, Double>(clusterOfPoint, dist);
			}
		}).foreach(new VoidFunction<Tuple2<Integer, Double>>() {

			@Override
			public void call(Tuple2<Integer, Double> t) throws Exception {
				accumSum.add(Math.round(t._2));
				accumElements.add(1);
			}
		});
		double estimatedTime = (System.nanoTime() - startTime)/ 1000000000.0;
		System.out.println("\n\n>>>>>>>>>>>>>>>>>>>>>>>>> Quality Score with " + k + " Cluster <<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
		System.out.println("Score: " + accumSum.value().doubleValue() / accumElements.value());
		System.out.println("\nElapsed Time: " + estimatedTime + " seconds");
		System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>> Quality Score with " + k + " Cluster <<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
		sc.close();
	}
	
	public static void kMeansClusteringDefault(SparkConf conf, String path) {
		JavaSparkContext sc = new JavaSparkContext(conf);
		LongAccumulator accumSum = sc.sc().longAccumulator();
		LongAccumulator accumElements = sc.sc().longAccumulator();
		
		long startTime = System.nanoTime();
//		String fullPath = path + "//src//main//resources//kddcup.data_10_percent_corrected.fin";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
			public Vector call(String s) {
				String[] sarray = s.split(",");
				double[] values = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++) {
					values[i] = Double.parseDouble(sarray[i]);
				}
				return Vectors.dense(values);
			}
		});

		parsedData.cache();
		KMeans kmeans = new KMeans();
		final KMeansModel clusters = kmeans.run(parsedData.rdd());
		final Vector[] centers = clusters.clusterCenters();

		/**
		 * mapToPair = Distance from data point to nearest Cluster foreach = Sum
		 * of all distances and elements
		 */
		parsedData.mapToPair(new PairFunction<Vector, Integer, Double>() {
			public Tuple2<Integer, Double> call(Vector vector) throws Exception {
				int clusterOfPoint = clusters.predict(vector);
				double dist = Math.sqrt(Vectors.sqdist(vector, centers[clusterOfPoint]));
				return new Tuple2<Integer, Double>(clusterOfPoint, dist);
			}
		}).foreach(new VoidFunction<Tuple2<Integer, Double>>() {

			@Override
			public void call(Tuple2<Integer, Double> t) throws Exception {
				accumSum.add(Math.round(t._2));
				accumElements.add(1);
			}
		});
		double estimatedTime = (System.nanoTime() - startTime)/ 1000000000.0;
		System.out.println("\n\n>>>>>>>>>>>>>>>>>>>>>>>>> Quality Score with default Settings <<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
		System.out.println("Score: " + accumSum.value().doubleValue() / accumElements.value());
		System.out.println("\nElapsed Time: " + estimatedTime + " seconds");
		System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>> Quality Score with default Settings <<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
		sc.close();
	}
	
	public static void initLocal(SparkConf conf){
		conf.setAppName("GRUPPE01").setMaster("local[*]").set("spark.executor.memory", "6g")
				.set("spark.driver.memory", "2g");
		System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>> Hello from Spark in local mode <<<<<<<<<<<<<<<<<<<<<<<<<\n");
	}
	
	public static void initEMR(SparkConf conf){
		// on AWS:
				// SparkConf conf = new SparkConf().setAppName("GRUPPE01");
				// String AWS_ACCESS_KEY_ID = "";
				// String AWS_SECRET_ACCESS_KEY = "";
		
		// ...example accessing S3:

				// clusterMembers.saveAsTextFile("s3n://" + AWS_ACCESS_KEY_ID + ":" +
				// AWS_SECRET_ACCESS_KEY + "@qltrail-lab-265-1488270472/result");
		
		// spark.driver.cores -> Number of cores to use for the driver process,
				// only in cluster mode
	}

}
