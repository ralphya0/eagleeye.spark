package com.yaoxin.eagleeye.spark.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.yaoxin.eagleeye.spark.core.biz.BusinessLogic;
import com.yaoxin.eagleeye.spark.core.biz.impl.AbnormalIntervalMatcher;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public class OfflineProcessor {

	public final static int SLEEP_INTERVAL = 30000;
	public final static String APP_NAME = "EagleEye.Spark";
	public final static String SPARK_CLUSTER_MASTER = "spark://59.67.152.231";
	// public final static int CLEANER_SCHEDULE_PERIOD = 24;
	static List<BusinessLogic> biz = new ArrayList<BusinessLogic>();
	
	public static void init(){
		biz.add(new AbnormalIntervalMatcher());
		
		// 只在生产环境中启用cleaner : )
		// Timer timer = new Timer();
		// timer.schedule(new HistoryDataCleaner(), new Date(), CLEANER_SCHEDULE_PERIOD);
	}
	
	public static void prepare(){
		for(BusinessLogic b : biz){
			b.prepare();
		}
	}
	
	public static void cleanup(){
		for(BusinessLogic b : biz){
			b.cleanup();
		}
	}
	
	public static void main(String[] args) {
		init();
		prepare();
		
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(SPARK_CLUSTER_MASTER);
		JavaSparkContext ctx = new JavaSparkContext(conf);
		
		try {
			
			while(true){
				
				for(BusinessLogic b : biz){
					b.schedule(ctx);
				}
				
				// 等待下一分钟netflow数据的到来
				Thread.sleep(SLEEP_INTERVAL);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			BufferedWriter bw;
			try {
				bw = new BufferedWriter(new FileWriter("/home/yaoxin/logs/spark/log.txt", true));
				bw.write(e.getMessage() + "\n");
				bw.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		finally{
			cleanup();
		}
	}
}
