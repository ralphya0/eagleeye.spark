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

	public RealtimeIndicatorsVO getBackwardRealtimeIndicator();
	
	public void markBackwardRawRecord(String timeFrame, String id);
	
	public void markBackwardRealtimeIndicators(String timeFrame, String id);
	
	public List<NetflowRecord> getBackwardAbnormalRawRecord(String abnormalId);
	
	public List<RealtimeIndicatorsVO> getBackwardAbnormalRealtimeIndicators(String abnormalId);
	
	public void unmarkBackwardRawRecordById(String id);
	
	public void unmarkBackwardRealtimeIndicatorsById(String id);
	
	public void unmarkBackwardRawRecordByInterval(String beginTime, String endTime, String id);
	
	public void unmarkBackwardRealtimeIndicatorsByInterval(String beginTime, String endTime, String id);
	
	public void addAbnormalRecord(AbnormalTrafficRecord r);
	
	public List<NetflowRecord> getBackwardRawRecordByInterval(String beginTime, String endTime);
	
	public List<RealtimeIndicatorsVO> getBackwardRealtimeIndicatorsByInterval(String beginTime, String endTime);
	
	public void doClean(String sql);
	
	
	//public void addTmpTrafficRecord(AbnormalTrafficRecord r);
	
}
