package com.yaoxin.eagleeye.spark.core.biz;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月17日
 */
public interface BusinessLogic {

	public void prepare();
	public void schedule(JavaSparkContext ctx);
	public void cleanup();
}
