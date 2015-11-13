package com.yaoxin.eagleeye.spark.util;

import java.util.Comparator;

import com.yaoxin.eagleeye.spark.vo.RealtimeIndicatorsVO;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月13日
 */
public class PpfComparator implements Comparator<RealtimeIndicatorsVO>{

	public int compare(RealtimeIndicatorsVO o1, RealtimeIndicatorsVO o2) {
		try {
			return o1.getPpf() - o2.getPpf();
		} catch (Exception e) {
		}
		return 0;
	}

}
