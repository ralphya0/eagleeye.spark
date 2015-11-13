package com.yaoxin.eagleeye.spark.core;


/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public class OfflineProcessor {

	public final static int SLEEP_INTERVAL = 20000;
	public final static int CHECKPOINT_INTERVAL = 3600000;
	
	
	public static void main(String[] args) {
		AbnormalIntervalMatcher abnormalMatcher = new AbnormalIntervalMatcher();
		
		while(true){
			
			try {
				
				// 等待下一分钟indicators的到来
				Thread.sleep(SLEEP_INTERVAL);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
