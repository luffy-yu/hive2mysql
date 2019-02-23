package com.homework3.ylc.hive2mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class QueryHive {
	
	private final SparkSession spark;
	private final JavaSparkContext jsc;
	private final HiveContext hiveContext;
	private final OperateMySQL opmysql;
	
	public QueryHive(String appName)
	{
//    	//in dev mode
//      spark = SparkSession
//      	      .builder()
//      	      .appName("HbaseDemo")
//      	      .master("spark://cluster1:7077")
//      	      .config("spark.testing.memory", "471859200")
//      	      .getOrCreate();
      //in deploy mode
      spark = SparkSession
    	      .builder()
    	      .appName(appName)
    	      .getOrCreate();
      jsc = new JavaSparkContext(spark.sparkContext());
      hiveContext = new org.apache.spark.sql.hive.HiveContext(jsc);
      String url = "jdbc:mysql://cluster2:3306/homework3";
      String username = "root";
      String password = "root";
      
      opmysql = new OperateMySQL(url, username, password);
		
	}
	
	static Function<Row, String> myFunc = new Function<Row, String>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public String call(Row v1) throws Exception {
			// TODO Auto-generated method stub
			// 倒数第二个字段是日期
			int length = v1.length();
			return v1.getString(length - 2);
		}
		
	};
	
	
	public UserBehavior object2UserBehavior_uid(Row r)
	{
//		Row r = (Row)o;
		//uid STRING
		//behavior_total INT
		//behavior_publish INT
		//behavior_view INT
		//behavior_comment INT
		//day_time STRING
		//hour_time INT
		
		String uid = r.getString(0);
		String publish = String.valueOf(r.getInt(2));
		String view = String.valueOf(r.getInt(3));
		String comment = String.valueOf(r.getInt(4));
		String day_time = r.getString(5);
		
		UserBehavior ub = UserBehavior.create(uid, publish, view, comment, "", day_time);
		return ub;
	}
	
	public UserBehavior object2UserBehavior_aid(Row r)
	{
//		Row r = (Row)o;
		//aid STRING
		//behavior_total INT
		//behavior_publish INT
		//behavior_view INT
		//behavior_comment INT
		//day_time STRING
		//hour_time INT
		
		String aid = r.getString(0);
		String publish = String.valueOf(r.getInt(2));
		String view = String.valueOf(r.getInt(3));
		String comment = String.valueOf(r.getInt(4));
		String day_time = r.getString(5);
		
		UserBehavior ub = UserBehavior.create("", publish, view, comment, aid, day_time);
		return ub;
	}
	
	public void processUid(String tablename)
	{
		System.err.println("Process Uid: " + tablename);
		String sql = String.format("select * from %s", tablename);
		Dataset<Row> results = hiveContext.sql(sql);
		
		JavaRDD<Row> rdd = results.toJavaRDD();
		JavaPairRDD<String, Iterable<Row>> grouped = rdd.groupBy(myFunc);
		JavaRDD<String> keysRDD = grouped.keys();
		List<String> keys = keysRDD.collect();
		// 去重
		List<Object> keys2 = keys.stream().distinct().collect(Collectors.toList());
		System.err.println("Total:" + keys2.size());
		int total = keys2.size();
		int i = 0;
		// 按照key（daytime）获取
		for(Object obj : keys2)
		{
			i += 1;
			System.err.println(" Current:" + i + " / " + total);
			String key = obj.toString();
			List<Iterable<Row>> lookuped = grouped.lookup(key);
            List<Tuple2<String, UserBehavior>> groupedRDD = new ArrayList<Tuple2<String, UserBehavior>>();
            for(Iterable<Row> rows: lookuped)
            {
            	for(Row row: rows)
            	{
                	UserBehavior ub = object2UserBehavior_uid(row);
                	groupedRDD.add(new Tuple2<String, UserBehavior>(key, ub));	
            	}
            }
            DayCount.run_uid(jsc, key, groupedRDD, opmysql);
		}
	}
	
	public void processAid(String tablename)
	{
		System.err.println("Process Aid: " + tablename);
		String sql = String.format("select * from %s", tablename);
		Dataset<Row> results = hiveContext.sql(sql);
		
		JavaRDD<Row> rdd = results.toJavaRDD();
		JavaPairRDD<String, Iterable<Row>> grouped = rdd.groupBy(myFunc);
		JavaRDD<String> keysRDD = grouped.keys();
		List<String> keys = keysRDD.collect();
		// 去重
		List<Object> keys2 = keys.stream().distinct().collect(Collectors.toList());
		System.err.println("Total:" + keys2.size());
		int total = keys2.size();
		int i = 0;
		// 按照key（daytime）获取
		for(Object obj : keys)
		{
			i += 1;
			System.err.println(" Current:" + i + " / " + total);
			String key = obj.toString();
			List<Iterable<Row>> lookuped = grouped.lookup(key);
            List<Tuple2<String, UserBehavior>> groupedRDD = new ArrayList<Tuple2<String, UserBehavior>>();
            for(Iterable<Row> rows: lookuped)
            {
            	for(Row row: rows)
            	{
                	UserBehavior ub = object2UserBehavior_aid(row);
                	groupedRDD.add(new Tuple2<String, UserBehavior>(key, ub));	
            	}
            }
            DayCount.run_aid(jsc, key, groupedRDD, opmysql);
		}
	}
}
