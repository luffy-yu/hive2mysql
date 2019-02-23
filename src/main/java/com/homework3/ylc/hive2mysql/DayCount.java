package com.homework3.ylc.hive2mysql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DayCount implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private UserBehavior _userBehavior;
    private int _total;//总数
	private static final String hive_url = "jdbc:hive2://cluster1:10000/default";
	private static final String hive_user = "hive";
	private static final String hive_pwd = "hive";
    public DayCount(UserBehavior user,int total){
        _userBehavior=user;
        _total=total;
    }
    public String toString(){
        return _userBehavior.toString() + " Total Count:" + String.valueOf(_total);
    }
    
    //createCombiner()
    static Function<UserBehavior,DayCount> createCombiner =new Function<UserBehavior,DayCount>(){
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public DayCount call(UserBehavior x){
            return new DayCount(x,1);
        }
    };
    
    //mergeValue()
    static Function2<DayCount,UserBehavior,DayCount> mergeValue_Uid=new Function2<DayCount, UserBehavior, DayCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public DayCount call(DayCount a, UserBehavior x) throws Exception {
            a._userBehavior.addByUid(x);
            a._total+=1;
            return a;
        }
    };
    //mergeValue2()
    static Function2<DayCount,UserBehavior,DayCount> mergeValue_Aid=new Function2<DayCount, UserBehavior, DayCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public DayCount call(DayCount a, UserBehavior x) throws Exception {
            a._userBehavior.addByAid(x);
            a._total+=1;
            return a;
        }
    }; 
    
    //mmergeCombiners()
    static Function2<DayCount,DayCount,DayCount> mergeCombiner_Uid=new Function2<DayCount, DayCount, DayCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public DayCount call(DayCount a, DayCount b) throws Exception {
            a._userBehavior.addByUid(b._userBehavior);;
            a._total+=b._total;
            return a;
        }
    };
    
    //mmergeCombiners2()
    static Function2<DayCount,DayCount,DayCount> mergeCombiner_Aid=new Function2<DayCount, DayCount, DayCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public DayCount call(DayCount a, DayCount b) throws Exception {
            a._userBehavior.addByAid(b._userBehavior);;
            a._total+=b._total;
            return a;
        }
    };
    
    /*
     * 将UserBehavior序列转换为以Uid为主键的List,
     * 用于按照用户统计用户的行为。
     * 用Uid作为主键，UserBehavior对象作为值
     */
    public static List<Tuple2<String,UserBehavior>> userBehaviorListByUid(Iterable<UserBehavior> in)
    {
    	List<Tuple2<String,UserBehavior>> ret = new ArrayList<Tuple2<String, UserBehavior>>();
    	
    	for(UserBehavior ub: in)
    	{
    		ret.add(new Tuple2<String, UserBehavior>(ub.Uid, ub));
    	}
    	
    	return ret;
    }
    
    /*
     * 将UserBehavior序列转换为以Aid为主键的List,
     * 用于按照文章统计用户的行为。
     * 用Aid作为主键，UserBehavior对象作为值。
     */
    public static List<Tuple2<String,UserBehavior>> userBehaviorListByAid(Iterable<UserBehavior> in)
    {
    	List<Tuple2<String,UserBehavior>> ret = new ArrayList<Tuple2<String, UserBehavior>>();
    	
    	for(UserBehavior ub: in)
    	{
    		ret.add(new Tuple2<String, UserBehavior>(ub.Aid, ub));
    	}
    	
    	return ret;
    }
    
    public static void clearFile(String filename)
    {
    	FileWriter fw = null;
		try {
			fw = new FileWriter(filename,false);
	    	fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
    }
    
    public static void write_aid_day(String filename, UserBehavior ub)
    {
    	try
    	{
    	    FileWriter fw = new FileWriter(filename,true); //append
    	    Integer total = (int)(ub.publish + ub.view + ub.comment);
    	    
    	    String line = String.format("%s\001%d\001%d\001%d\001%d\001%s\001%s\n", 
    	    		ub.Aid,total,ub.publish,ub.view,ub.comment,ub.BehaviorTime);
    	    
    	    fw.write(line);
    	    fw.close();
    	}
    	catch(Exception ioe)
    	{
    	    System.err.println("write_aid_hour Exception: " + ioe.getMessage());
    	    ioe.printStackTrace(System.err);
    	}
    }
    
    public static void write_uid_day(String filename, UserBehavior ub)
    {
    	try
    	{
    	    FileWriter fw = new FileWriter(filename,true); //append
    	    Integer total = (int)(ub.publish + ub.view + ub.comment);
    	    
    	    String line = String.format("%s\001%d\001%d\001%d\001%d\001%s\001%s\n", 
    	    		ub.Uid,total,ub.publish,ub.view,ub.comment,ub.BehaviorTime);
    	    fw.write(line);
    	    fw.close();
    	}
    	catch(Exception ioe)
    	{
    	    System.err.println("write_uid_hour Exception: " + ioe.getMessage());
    	    ioe.printStackTrace(System.err);
    	}
    }
    
    public static void run_uid(JavaSparkContext sc, String key, List<Tuple2<String,UserBehavior>> list, OperateMySQL op)
    {
        JavaPairRDD<String,UserBehavior> users= sc.parallelizePairs(list);
    	// 第一步按照时间分类统计用户的行为
        // 按照时间key做group运算
        JavaPairRDD<String, Iterable<UserBehavior>> grouped = users.groupByKey();
        // 将结果按照key(时间)-value(UserBehavior集合)的形式聚集在一起
        Map<String, Iterable<UserBehavior>> groupedMap = grouped.collectAsMap();
        // 第二步按照时间内的用户的行为
        // 按照key遍历，value为用户的行为集合
        for(Map.Entry<String, Iterable<UserBehavior>> entry: groupedMap.entrySet())
        {
        	// 转换为以Uid为主键的List
            List<Tuple2<String,UserBehavior>> uidUsers = userBehaviorListByUid(entry.getValue());
            // 
            JavaPairRDD<String,UserBehavior> parallelized = sc.parallelizePairs(uidUsers);
            // 通过key(Uid)合并
            JavaPairRDD<String,DayCount> Combined=parallelized.combineByKey(createCombiner,mergeValue_Uid,mergeCombiner_Uid);
            // 收集结果
            Map<String,DayCount> countMap= Combined.collectAsMap();
            for(Map.Entry<String,DayCount> entry2:countMap.entrySet())
            {
            	// 将结果是写出hive数据库
//            	System.err.println("Uid Key:"+ key +  " value:"+entry2.getValue()._userBehavior.toString());
            	op.insertInto_userbehaviorday_uid(entry2.getValue()._userBehavior, key);
            }      
        }
    }
    
    public static void run_aid(JavaSparkContext sc, String key, List<Tuple2<String,UserBehavior>> list, OperateMySQL op)
    {
        JavaPairRDD<String,UserBehavior> users= sc.parallelizePairs(list);
    	// 第一步按照时间分类统计用户的行为
        // 按照时间key做group运算
        JavaPairRDD<String, Iterable<UserBehavior>> grouped = users.groupByKey();
        // 将结果按照key(时间)-value(UserBehavior集合)的形式聚集在一起
        Map<String, Iterable<UserBehavior>> groupedMap = grouped.collectAsMap();
        // 第二步按照时间内的用户的行为
        // 按照key遍历，value为用户的行为集合
        for(Map.Entry<String, Iterable<UserBehavior>> entry: groupedMap.entrySet())
        {
            //转换为以Aid为主键的List
            List<Tuple2<String,UserBehavior>> aidUsers = userBehaviorListByAid(entry.getValue());
            // 
            JavaPairRDD<String,UserBehavior> parallelized_aid = sc.parallelizePairs(aidUsers);
            // 通过key(Uid)合并
            JavaPairRDD<String,DayCount> combined_aid=parallelized_aid.combineByKey(createCombiner,mergeValue_Aid,mergeCombiner_Aid);
            // 收集结果
            Map<String,DayCount> countMap_aid= combined_aid.collectAsMap();
            for(Map.Entry<String,DayCount> entry2:countMap_aid.entrySet())
            {
            	// 将结果是写出hive数据库
//            	System.err.println("Aid Key:"+ key +  " value:"+entry2.getValue()._userBehavior.toString());
            	op.insertInto_userbehaviorday_aid(entry2.getValue()._userBehavior, key);
            }
            
        }
    }
    
    public static void test()
    {
    	//in dev mode
        SparkSession spark = SparkSession
      	      .builder()
      	      .appName("AvgCount")
      	      .master("local")
      	      .config("spark.testing.memory", "471859200")
      	      .getOrCreate();
        
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        List<Tuple2<Integer,UserBehavior>> list=new ArrayList<Tuple2<Integer, UserBehavior>>();
        // key为时间，value为原始值
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("1", "1", "0", "0", "A", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("1", "0", "1", "0", "B", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("2", "0", "1", "0", "C", "0002")));
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("2", "0", "0", "1", "A", "0002")));
        list.add(new Tuple2<Integer, UserBehavior>(2,UserBehavior.create("1", "1", "0", "0", "B", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(2,UserBehavior.create("1", "0", "0", "1", "D", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(2,UserBehavior.create("1", "0", "0", "1", "A", "0001")));
        
//        run(sc, list, QueryHBase.hourly_uid_file_local, QueryHBase.hourly_aid_file_local);
        
        spark.stop();
        sc.close();
    }
    
    public static void main(String args[]){
    	
    	test();
    	
//    	UserBehavior ub = UserBehavior.create("C", "1", "1", "0", "A1", " 2015-01-09  16:03:41");
    	
//    	write_aid_hour(QueryHBase.hourly_aid_file_local, ub);
//    	write_uid_hour(QueryHBase.hourly_uid_file_local, ub);
    }
}
