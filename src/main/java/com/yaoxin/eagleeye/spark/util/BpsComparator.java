package com.yaoxin.eagleeye.spark.util;

import java.util.Comparator;

import com.yaoxin.eagleeye.spark.vo.RealtimeIndicatorsVO;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月13日
 */
public class BpsComparator implements Comparator<RealtimeIndicatorsVO>{

	public int compare(RealtimeIndicatorsVO o1, RealtimeIndicatorsVO o2) {
		try {
			
			return (int) (o1.getBps() - o2.getBps());
		} catch (Exception e) {
		}
		return 0;
	}

}
