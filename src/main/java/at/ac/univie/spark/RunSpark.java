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
		KMeansModel clusters = kmeans.run(parsedData.rdd());
		System.out.println("Cluster centers:");
		for(Vector center:clusters.clusterCenters()){
			System.out.println("Cluster: " + center);
		}
		
		
//		System.out.println(">>>>>>>>>>>>>>>>>> Number of Elements: " + parsedData.count() + " <<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n");
//		System.out.println(">>>>>>>>>>>>>>>>>> Before Collect <<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n");
//		
//		List<Partition> partitionList = parsedData.coalesce(100000,true).partitions();
//		System.out.println(">>>>>>>>>>>>>>>>>> Number of Partitions: " + partitionList.size() + " <<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n");
//		
//		for(Partition part: partitionList){
//			int idx = part.index();
//			JavaRDD<Vector> partRdd = parsedData.mapPartitionsWithIndex(new Function2<Integer, Iterator<Vector>, Iterator<Vector>>(
//					) {
//						@Override
//						public Iterator<Vector> call(Integer v1, Iterator<Vector> v2) throws Exception {
//							if(v2.hasNext()){
//								v2.next();
//								return v2;
//							}else{
//								return v2;
//							}
//						}
//			} , true);
//			partRdd.cache();
//			List<Vector> list = partRdd.collect();
//			System.out.println(">>>>>>>>>>>>>>>>>> Number of CollectElements of "+idx+": " + list.size() + " <<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n");
//		}
		//parsedData.zipWithIndex().filter((Tuple2<Row,Long> v1) -> v1._2 >= 0 && v1._2 < 1);

//		Iterator<Vector>iterator = parsedData.toLocalIterator();
//		while(iterator.hasNext()){
//			System.out.println("Cluster: " + clusters.predict(iterator.next()) + " " + iterator.next().toString());
//		}
		System.out.println(">>>>>>>>>>>>>>>>>> After Collect <<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n");
		
		sc.close();
		
		
	}
	
	public double euclidianDist(Vector point){
		double[] dPoint = point.toArray();
		double sum = 0;
		for(int i=0;i<dPoint.length;i++){
			//-Centroid is missing
			sum+=Math.pow(dPoint[i], 2);
		}
		double dist=(double)Math.sqrt(sum);
		return dist;
	}

}
