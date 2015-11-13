package com.yaoxin.eagleeye.spark.util;

import java.util.Comparator;

import com.yaoxin.eagleeye.spark.vo.NetflowRecord;

import scala.Tuple2;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月13日
 */
public class NetflowRecordComparator implements Comparator<Tuple2<String,NetflowRecord>>{

	public int compare(Tuple2<String, NetflowRecord> o1, Tuple2<String, NetflowRecord> o2) {
		try {
			return (int) (o1._2.getBytes() - o2._2.getBytes());
		} catch (Exception e) {
		}
		return 0;
	}



}
