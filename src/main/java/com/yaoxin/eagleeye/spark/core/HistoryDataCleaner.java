package com.yaoxin.eagleeye.spark.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;

import com.yaoxin.eagleeye.spark.storage.StorageManager;
import com.yaoxin.eagleeye.spark.storage.impl.MysqlStorageManager;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月15日
 */
public class HistoryDataCleaner extends TimerTask{

	public final static String CLEANUP_BACKWARD_RAW_RECORD = 
			"delete from backward_raw_records where m_date < '@' and no_deletion = 0";
	public final static String CLEANUP_BACKWARD_INDICATORS = 
			"delete from backward_realtime_indicators where time_frame < '@' and no_deletion = 0";
	public final static String CLEANUP_BACKWARD_SESSIONS = 
			"delete from unidirectional_backward_session_record where start_time < '@'";
	public final static String CLEANUP_FORWARD_RAW_RECORD = 
			"delete from forward_raw_records where m_date < '@' and no_deletion = 0";
	public final static String CLEANUP_FORWARD_INDICATORS = 
			"delete from forward_realtime_indicators where time_frame < '@' and no_deletion = 0";
	public final static String CLEANUP_FORWARD_SESSIONS = 
			"delete from unidirectional_forward_session_record where start_time < '@'";
	
	public final static StorageManager storageManager = new MysqlStorageManager();
	public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	

	@Override
	public void run() {
		try {
			Calendar calendar = Calendar.getInstance();
			Date now = new Date();
			calendar.setTime(now);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.add(Calendar.HOUR, -24);
			
			String date = sdf.format(calendar.getTime());
			
			storageManager.doClean(CLEANUP_BACKWARD_RAW_RECORD.replace("@", date));
			storageManager.doClean(CLEANUP_BACKWARD_INDICATORS.replace("@", date));
			storageManager.doClean(CLEANUP_BACKWARD_SESSIONS.replace("@", date));
			storageManager.doClean(CLEANUP_FORWARD_RAW_RECORD.replace("@", date));
			storageManager.doClean(CLEANUP_FORWARD_INDICATORS.replace("@", date));
			storageManager.doClean(CLEANUP_FORWARD_SESSIONS.replace("@", date));
			
		} catch (Exception e) {
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
	}
}
