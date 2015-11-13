package com.yaoxin.eagleeye.spark.storage;

import java.util.List;

import com.yaoxin.eagleeye.spark.vo.AbnormalTrafficRecord;
import com.yaoxin.eagleeye.spark.vo.NetflowRecord;
import com.yaoxin.eagleeye.spark.vo.RealtimeIndicatorsVO;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public interface StorageManager {

	public RealtimeIndicatorsVO getRealtimeIndicator();
	
	public void markRawRecord(String timeFrame, String id);
	
	public void markRealtimeIndicators(String timeFrame, String id);
	
	public List<NetflowRecord> getAbnormalRawRecord(String abnormalId);
	
	public List<RealtimeIndicatorsVO> getAbnormalRealtimeIndicators(String abnormalId);
	
	public void unmarkRawRecordById(String id);
	
	public void unmarkRealtimeIndicatorsById(String id);
	
	public void unmarkRawRecordByInterval(String beginTime, String endTime, String id);
	
	public void unmarkRealtimeIndicatorsByInterval(String beginTime, String endTime, String id);
	
	public void addAbnormalRecord(AbnormalTrafficRecord r);
	
	public List<NetflowRecord> getRawRecordByInterval(String beginTime, String endTime);
	
	public List<RealtimeIndicatorsVO> getRealtimeIndicatorsByInterval(String beginTime, String endTime);
	
	public void addTmpTrafficRecord(AbnormalTrafficRecord r);
	
}
