
package edu.mum.bigdata.spark.mo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import com.esotericsoftware.minlog.Log;

import scala.Tuple2;
import scala.Tuple3;

public class App {
	private static Logger log = Logger.getLogger(App.class);
	
	
	
	public static ArrayList<Tuple2<String,String>> findNeighbourPairs(String w, List<String> list){
		if(list.size() == 0){
			return new ArrayList<Tuple2<String,String>>();
		}
        if(list.size() == 1){
        	Tuple2<String,String> gp = new Tuple2<String,String>( new String(w) , new String(list.remove(0)) );
            return new ArrayList<Tuple2<String,String>>(Arrays.asList(gp));
        }

        ArrayList<Tuple2<String,String>> ret = new ArrayList<Tuple2<String,String>>();
            for(String w2:list){
                if(w2.equals(w)){
                    break;
                }
                ret.add(new Tuple2<String,String>(w, w2));
            }

        ret.addAll( findNeighbourPairs(list.remove(0), list) );
        return ret;
}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"DataCo Relative CoOccurence Count");
		JavaSparkContext sc = new JavaSparkContext(conf);

		log.info("Starting app ...");
		
		JavaPairRDD<String, List<String>> ipProductPairs = sc
				.textFile(args[0])
				.map(new Function<String, Tuple2<String, String>>() {
					public Tuple2<String, String> call(String line)
							throws Exception {
						ApacheAccessLog parser = ApacheAccessLog.parseFromLogLine(line);
						
						String endpoint = parser.getEndpoint();
						endpoint = URLDecoder.decode(endpoint, "UTF-8");
						//log.error("endpoint is : " + endpoint);
						return new Tuple2<String, String>(
								parser.getIpAddress(), endpoint.toLowerCase());
					}
				})
				.filter(new Function<Tuple2<String, String>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, String> item)
							throws Exception {
						return item._2.contains("/product/")
								&& item._2.contains("/add_to_cart");
					}

				})
				.map(new Function<Tuple2<String, String>, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> item) throws Exception {
						String product = item._2
								.substring(
										item._2.lastIndexOf("/product/")
												+ "/product".length())
								.replace("/add_to_cart", "").replace("/", "");

						return new Tuple2<String, String>(item._1, product);
					}

				})
				.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, String> arg0)  {
						List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
						ret.add(arg0);
						return ret.iterator();
					}
				})
				.mapToPair(
						new PairFunction<Tuple2<String, String>, String, List<String>>() {
							@Override
							public Tuple2<String, List<String>> call(
									Tuple2<String, String> item)
									throws Exception {
								List<String> ar = new ArrayList<String>();
								ar.add(item._2());
								return new Tuple2<String, List<String>>(item._1(), ar);
							}
						});
		
		
		JavaPairRDD<Tuple2<String, String>, Integer> neighbours = ipProductPairs.reduceByKey(
		new Function2<List<String>, List<String>, List<String>>(){
			@Override
			public List<String> call(List<String> v1, List<String> v2)
					throws Exception {
				List<String> prod = new ArrayList<String>();
				prod.addAll(v1);
				prod.addAll(v2);
				return prod;
			}
			
		}).map(new Function<Tuple2<String, List<String>>, List<Tuple3<String,  String, Integer>>>(){
			@Override
			public List<Tuple3<String, String, Integer>> call(
					Tuple2<String, List<String>> record) throws Exception {		
				ArrayList<Tuple2<String, String>> neighbors = findNeighbourPairs(record._2().remove(0), record._2());
				List<Tuple3<String,  String, Integer>> ret = new ArrayList<Tuple3<String,  String, Integer>>();
				for(Tuple2<String, String> gp:neighbors){		
		            ret.add(new Tuple3<String, String,Integer>(gp._1(), gp._2(), 1));
		            ret.add(new Tuple3<String, String,Integer>(gp._1(), "*", 1));
		        }
				return ret;
			}}).flatMap(new FlatMapFunction<List<Tuple3<String, String, Integer>>, Tuple3<String, String, Integer>>(){

				@Override
				public Iterator<Tuple3<String, String, Integer>> call(
						List<Tuple3<String, String, Integer>> t)
						throws Exception {
					List<Tuple3<String, String, Integer>> al = new ArrayList<Tuple3<String, String, Integer>>();
					for(Tuple3<String, String, Integer> ta:t){
						al.add(ta);
					}
					return al.iterator();
				}
		
			})/*.filter(new Function<Tuple3<String, String, Integer>, Boolean>(){

				@Override
				public Boolean call(Tuple3<String, String, Integer> v1)
						throws Exception {
					return v1._1().contains("yakima doubledown ace hitch mount 4-bike rack");
				}
			})*/.map(new Function<Tuple3<String, String, Integer>, Tuple2<Tuple2<String, String>, Integer>>(){

				@Override
				public Tuple2<Tuple2<String, String>, Integer> call(
						Tuple3<String, String, Integer> v1) throws Exception {
					return new Tuple2<Tuple2<String, String>, Integer>(
							new Tuple2<String, String>(v1._1(), v1._2()),v1._3());
				}}).mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, String>, Integer>(){
					@Override
					public Tuple2<Tuple2<String, String>, Integer> call(
							Tuple2<Tuple2<String, String>, Integer> arg0)
							throws Exception {
						return new Tuple2<Tuple2<String, String>, Integer>(arg0._1(), arg0._2());
					}
					
				});
		  
		  final JavaPairRDD<Tuple2<String, String>, Integer> neighboursSorted = neighbours
				  .reduceByKey(new Function2<Integer, Integer, Integer>(){
						@Override
						public Integer call(Integer v1, Integer v2) throws Exception {
							return v1+v2;
						}})
				.sortByKey(new KeyComparator()).cache();
		  
		  
		  neighboursSorted
				.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Integer>>(){
					@Override
					public void call(Tuple2<Tuple2<String, String>, Integer> t)
							throws Exception {
						 	if(t._1()._2().equals("*")){
						 		Log.info("Got * in " + t._1() + " val: " + t._2());
						 		CountHolder.lastCounter = t._2();
						 		return;
						 	}
						 	Log.info("Calculating relative val for  " + t._1() + " using lastCounter: " + CountHolder.lastCounter);
							float ratio = (1.f * t._2())/( 1.f * CountHolder.lastCounter );
							DecimalFormat df = new DecimalFormat("#.00"); 
							ratio = Float.parseFloat(df.format(ratio));
							System.out.println(t._1() + " : " + ratio);
						
					}
				});

		  
		  JavaRDD<Tuple2<Tuple2<String, String>, Float>> freqSortedRatio = neighboursSorted.map(new Function<Tuple2<Tuple2<String, String>, Integer>, Tuple2<Tuple2<String, String>, Float>>(){

			@Override
			public Tuple2<Tuple2<String, String>, Float> call(
					Tuple2<Tuple2<String, String>, Integer> t)
					throws Exception {
				if(t._1()._2().equals("*")){
			 		CountHolder.lastCounter = t._2();
			 		return new Tuple2<Tuple2<String, String>, Float>(t._1(), 0.f);
			 	}
				float ratio = (1.f * t._2())/( 1.f * CountHolder.lastCounter );
				DecimalFormat df = new DecimalFormat("#.00"); 
				ratio = Float.parseFloat(df.format(ratio));
				System.out.println(t._1() + " : " + ratio);
				
				return new Tuple2<Tuple2<String, String>, Float>(t._1(), ratio);
			}
			  
		  }).filter(new Function<Tuple2<Tuple2<String, String>, Float>, Boolean>(){

			@Override
			public Boolean call(Tuple2<Tuple2<String, String>, Float> v1)
					throws Exception {
				return !v1._1()._2().equals("*");
			}});
		  
		  freqSortedRatio.saveAsTextFile("hdfs:///user/hive/warehouse/spark_prod_relfreq_" + System.currentTimeMillis());
		  
		  List<Tuple2<Tuple2<String, String>, Float>> top50byVal = freqSortedRatio.takeOrdered(50, new ValComparator());
		  System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
		  System.out.println("TOP Relations: ");
		  System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
		  for(Tuple2<Tuple2<String, String>, Float> pair:top50byVal) {
			  System.out.println(pair._1() + " -> " + pair._2());
		  }
	}
}
