package com.homework3.ylc.hive2mysql;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        System.out.println( "Hello World!" );
    	QueryHive qh = new QueryHive("hive2mysql");
    	qh.processUid("userbehaviorhour_uid");
    	qh.processAid("userbehaviorhour_aid");
    }
}
