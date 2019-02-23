package com.homework3.ylc.hive2mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OperateMySQL {
	
	private static String driverName = "com.mysql.jdbc.Driver";
	private Connection connection;
	private Statement statement;
	
	static
	{
		try {
			Class.forName(driverName);
			System.out.println("Driver loaded!");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Cannot find the driver in the classpath!", e);
		}
	}
	
	public OperateMySQL(String url, String user, String pwd)
	{
		try {
			connection = DriverManager.getConnection(url, user, pwd);
			statement = connection.createStatement();
			createTable();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
	}
	
	public void createTable()
	{
		String[] sqls = new String[] {"drop table if exists userbehaviorday_uid",
				"drop table if exists userbehaviorday_aid",
				"create table userbehaviorday_uid(uid varchar(255),behavior_total int,behavior_publish int,behavior_view int,behavior_comment int,day_time varchar(255))",
				"create table userbehaviorday_aid(aid varchar(255),behavior_total int,behavior_publish int,behavior_view int,behavior_comment int,day_time varchar(255))"
		};
		for(String sql : sqls)
		{
			try {
				statement.execute(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.err);
			}
		}
	}
	
	/**
	 * 插入数据到userbehaviorday_uid表
	 * 表的定义语句如下：
	 * create table userbehaviorday_uid
		(
		uid varchar(255),
		behavior_total int,
		behavior_publish int,
		behavior_view int,
		behavior_comment int,
		day_time varchar(255)
		);
	 * @param ub
	 * @throws SQLException 
	 */
	public void insertInto_userbehaviorday_uid(UserBehavior ub, String day)
	{		
		int total = (int)(ub.publish + ub.view + ub.comment);
		String sql = String.format("insert into userbehaviorday_uid"
				+ "(uid,behavior_total,behavior_publish,"
				+ "behavior_view,behavior_comment,"
				+ "day_time) values('%s',%d,%d,%d,%d,'%s')",ub.Uid,
				total, ub.publish, ub.view, ub.comment, day);
		System.out.println(sql);
		
		try {
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
	}
	/**
	 * 插入数据到userbehaviorday_aid表
	 * 表的定义语句如下：
	 * create table userbehaviorday_aid
		(
		aid varchar(255),
		behavior_total int,
		behavior_publish int,
		behavior_view int,
		behavior_comment int,
		day_time varchar(255)
		);
	 * @param ub
	 * @throws SQLException 
	 */
	public void insertInto_userbehaviorday_aid(UserBehavior ub, String day)
	{	
		int total = (int)(ub.publish + ub.view + ub.comment);
		String sql = String.format("insert into userbehaviorday_aid"
				+ "(aid,behavior_total,behavior_publish,"
				+ "behavior_view,behavior_comment,"
				+ "day_time) values('%s',%d,%d,%d,%d,'%s')",ub.Aid,
				total, ub.publish, ub.view, ub.comment, day);
		System.out.println(sql);
		
		try {
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
	}
	
	public void test()
	{   
		try {
			statement = connection.createStatement();
		    String sql = "select count(*) from test_table;";
		    
		    ResultSet res = statement.executeQuery(sql);
		    if(res.next()) {
		      System.out.println(res.getString(1));
		    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
	}

	public static void main(String []argv)
	{
		String url = "jdbc:mysql://cluster2:3306/homework3";
		String username = "root";
		String password = "root";
	    Statement stmt;
	    
	    OperateMySQL oms = new OperateMySQL(url,username,password);
	    oms.test();
	}
}
