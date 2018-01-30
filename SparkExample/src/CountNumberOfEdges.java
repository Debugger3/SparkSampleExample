
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
///import java.util.;

public class CountNumberOfEdges {	
	
	public static void main(String args[]) throws Exception{
		/*URI uri = URI.create("hdfs://localhost:54310/TestingHDFS/edges.txt");
		System.out.println("ok");
		Configuration conf = new Configuration ();
		FileSystem file = FileSystem.get (uri, conf);
		FSDataInputStream in = file.open(new Path(uri));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String line = null;
	    int verticesCount[]=new int[1696415];
		Arrays.fill(verticesCount,0);
	    while((line = br.readLine())!= null){
	    	line=line.trim();
	        String vertex[]=line.split("\t");
	        int v1=Integer.parseInt(vertex[0]);
			int v2=Integer.parseInt(vertex[1]);
			verticesCount[v1]++;
			verticesCount[v2]++;
	    }
	    in.close();
	    br.close();
	    file.close();
	    for(int i=0;i<verticesCount.length;i++)
			System.out.println( i+","+verticesCount[i]);
		}*/
	    JavaSparkContext sc=new JavaSparkContext(new SparkConf().setAppName("Vertex edge count").setMaster("local"));
	   
	    JavaRDD<String> input=sc.textFile("hdfs://localhost:54310/TestingHDFS/edges.txt");
	    System.out.println("read input");
	    JavaRDD<String> vertices=input.flatMap(new FlatMapFunction<String,String>(){
	    	@Override
	    	public Iterator<String> call(String x){
	    		return Arrays.asList(x.split("\t")).iterator();
	    	}});
	    JavaPairRDD<String,Integer> vertexEdgeCount=vertices.mapToPair(new PairFunction<String,String,Integer>(){
	    	public Tuple2<String,Integer> call(String x){
	    		return new Tuple2(x,1);
	    	}
	    }).reduceByKey(new Function2<Integer,Integer,Integer>(){
	    	public Integer call(Integer x,Integer y) {return x+y;}
	    });
	    
	    vertexEdgeCount.saveAsTextFile("test");
	    
	}
}
