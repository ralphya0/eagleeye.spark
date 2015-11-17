package com.yaoxin.eagleeye.spark.storage.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.yaoxin.eagleeye.spark.storage.StorageManager;
import com.yaoxin.eagleeye.spark.util.DBConnector;
import com.yaoxin.eagleeye.spark.vo.AbnormalTrafficRecord;
import com.yaoxin.eagleeye.spark.vo.NetflowRecord;
import com.yaoxin.eagleeye.spark.vo.RealtimeIndicatorsVO;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public class MysqlStorageManager implements StorageManager{
	
	public final static String LOAD_BACKWARD_REALTIME_INDICATOR = 
			"select * from backward_realtime_indicators order by time_frame desc limit 1";
	public final static String MARK_BACKWARD_RAW_RECORD = 
			"update backward_raw_records set no_deletion = 1, abnormal_id = '@' where m_date >= '#' and m_date < '&'";
	public final static String MARK_BACKWARD_REALTIME_INDICATORS = 
			"update backward_realtime_indicators set no_deletion = 1, abnormal_id = '@' where time_frame = '#'";
	public final static String LOAD_BACKWARD_ABNORMAL_RAW_RECORDS = 
			"select * from backward_raw_records where abnormal_id = '@'";
	public final static String LOAD_BACKWARD_ABNORMAL_INDICATORS = 
			"select * from backward_realtime_indicators where abnormal_id = '@'";
	public final static String UNMARK_BACKWARD_RAW_RECORD_BY_ID = 
			"update backward_raw_records set no_deletion = 0, abnormal_id = null where abnormal_id = '@'";
	public final static String UNMARK_BACKWARD_REALTIME_INDICATORS_BY_ID =
			"update backward_realtime_indicators set no_deletion = 0, abnormal_id = null where abnormal_id = '@'";
	public final static String UNMARK_BACKWARD_RAW_RECORD_BY_INTERVAL = 
			"update backward_raw_records set no_deletion = 0, abnormal_id = null where m_date >= '@' and m_date < '#' and abnormal_id = '$'";
	public final static String UNMARK_BACKWARD_REALTIME_INDICATORS_BY_INTERVAL =
			"update backward_realtime_indicators set no_deletion = 0, abnormal_id = null where time_frame >= '@' and time_frame < '#' and abnormal_id = '$'";
	public final static String ADD_ABNORMAL_RECORD = 
			"insert into abnormal_traffic_record(id, start_time, end_time, duration, start_point_deviation_ratio, "
			+ "end_point_deviation_ratio, priority, max_bps, min_bps, avg_bps, outter_ip_num, top_10_outter_ips, "
			+ "outter_ip_regions, history_accessor_ratio, max_record_creation_num, min_record_creation_num, "
			+ "avg_record_creation_num, max_bpp, min_bpp, avg_bpp, max_pps, min_pps, avg_pps, max_ppf, min_ppf,"
			+ "avg_ppf, max_bpf, min_bpf, avg_bpf, packets_less_than_500, pakcets_less_than_1000, packets_less_than_2000,"
			+ "packets_large_than_2000, bytes_less_than_20000, bytes_less_than_100000, bytes_large_than_100000,"
			+ "duration_less_than_1, duration_less_than_10, duration_large_than_10) values";
	public final static String LOAD_BACKWARD_RAW_RECORDS_BY_INTERVAL = 
			"select * from backward_raw_records where m_date >= '@' and m_date < '#'";
	public final static String LOAD_BACKWARD_ABNORMAL_INDICATORS_BY_INTERVAL = 
			"select * from backward_realtime_indicators where time_frame >= '@' and time_frame < '#'";
	public final static String ADD_TMP_TRAFFIC_RECORD = 
			"insert into tmp_traffic_record(start_time, end_time, duration, start_point_deviation_ratio, "
			+ "end_point_deviation_ratio, priority, max_bps, min_bps, avg_bps, outter_ip_num, top_10_outter_ips, "
			+ "outter_ip_regions, history_accessor_ratio, max_record_creation_num, min_record_creation_num, "
			+ "avg_record_creation_num, max_bpp, min_bpp, avg_bpp, max_pps, min_pps, avg_pps, max_ppf, min_ppf,"
			+ "avg_ppf, max_bpf, min_bpf, avg_bpf, packets_less_than_500, pakcets_less_than_1000, packets_less_than_2000,"
			+ "packets_large_than_2000, bytes_less_than_20000, bytes_less_than_100000, bytes_large_than_100000,"
			+ "duration_less_than_1, duration_less_than_10, duration_large_than_10) values";
	
	
	public RealtimeIndicatorsVO getBackwardRealtimeIndicator() {

		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(LOAD_BACKWARD_REALTIME_INDICATOR);
			
			if(rs != null && rs.next()){
				RealtimeIndicatorsVO vo = new RealtimeIndicatorsVO(rs.getInt(1), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getTime(2)), rs.getDouble(3), 
						rs.getDouble(6), rs.getInt(9),rs.getInt(10),rs.getInt(11),rs.getInt(12),rs.getInt(13),
						rs.getInt(14),rs.getInt(15),rs.getInt(16),rs.getInt(17),rs.getInt(18),rs.getInt(19),rs.getInt(20),
						rs.getInt(21),rs.getInt(22),rs.getInt(23));
				
				DBConnector.closeConn(conn, statement, rs);
				
				return vo;
			}
			
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
		return null;
	}

	public void markBackwardRawRecord(String timeFrame, String id) {
		try {
			
			String nTimeFrame = nextTimeFrame(timeFrame);
			Connection conn = DBConnector.getConn();
			
			String sql = MARK_BACKWARD_RAW_RECORD.replace("@", id).replace("#", timeFrame).replace("&", nTimeFrame);
			
			Statement statement = conn.createStatement();
			statement.execute(sql);
			DBConnector.closeConn(conn, statement, null);
			
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

	public void markBackwardRealtimeIndicators(String timeFrame, String id) {
		try {
			
			Connection conn = DBConnector.getConn();
			
			String sql = MARK_BACKWARD_REALTIME_INDICATORS.replace("@", id).replace("#", timeFrame);
			
			Statement statement = conn.createStatement();
			statement.execute(sql);
			DBConnector.closeConn(conn, statement, null);
			
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
	
	String nextTimeFrame(String current) throws ParseException{
		Calendar calendar = Calendar.getInstance();
		Date n = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(current);
		calendar.setTime(n);
		calendar.add(Calendar.MINUTE, 1);
		
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
	}

	public List<NetflowRecord> getBackwardAbnormalRawRecord(String abnormalId) {
		List<NetflowRecord> ret = new ArrayList<NetflowRecord>();
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = LOAD_BACKWARD_ABNORMAL_RAW_RECORDS.replace("@", abnormalId);
			
			ResultSet rs = statement.executeQuery(sql);
			while(rs.next()){
				NetflowRecord r = new NetflowRecord(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getTime(2)),
						rs.getDouble(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), 
						rs.getString(8), rs.getString(9), rs.getString(10), rs.getInt(11), (long)rs.getInt(12),
						rs.getInt(13), (long)rs.getInt(14), (long)rs.getInt(15), rs.getInt(16));
				
				ret.add(r);
			}
			DBConnector.closeConn(conn, statement, rs);
			
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
		return ret;
	}

	public List<RealtimeIndicatorsVO> getBackwardAbnormalRealtimeIndicators(String abnormalId) {
		List<RealtimeIndicatorsVO> ret = new ArrayList<RealtimeIndicatorsVO>();
		
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = LOAD_BACKWARD_ABNORMAL_INDICATORS.replace("@", abnormalId);
			ResultSet rs = statement.executeQuery(sql);
			
			while(rs.next()){
				RealtimeIndicatorsVO vo = new RealtimeIndicatorsVO(rs.getInt(1), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getTime(2)), rs.getDouble(3), 
						rs.getDouble(6), rs.getInt(9),rs.getInt(10),rs.getInt(11),rs.getInt(12),rs.getInt(13),
						rs.getInt(14),rs.getInt(15),rs.getInt(16),rs.getInt(17),rs.getInt(18),rs.getInt(19),rs.getInt(20),
						rs.getInt(21),rs.getInt(22),rs.getInt(23));
				
				ret.add(vo);
			}
			
			DBConnector.closeConn(conn, statement, rs);
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
		return ret;
	}

	public void unmarkBackwardRawRecordById(String id) {
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = UNMARK_BACKWARD_RAW_RECORD_BY_ID.replace("@", id);
			statement.execute(sql);
			
			DBConnector.closeConn(conn, statement, null);
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

	public void unmarkBackwardRealtimeIndicatorsById(String id) {
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = UNMARK_BACKWARD_REALTIME_INDICATORS_BY_ID.replace("@", id);
			statement.execute(sql);
			
			DBConnector.closeConn(conn, statement, null);
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


	public void unmarkBackwardRawRecordByInterval(String beginTime, String endTime, String id) {
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = UNMARK_BACKWARD_RAW_RECORD_BY_INTERVAL.replace("@", beginTime).replace("#", endTime).replace("$", id);
			statement.execute(sql);
			
			DBConnector.closeConn(conn, statement, null);
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

	public void unmarkBackwardRealtimeIndicatorsByInterval(String beginTime, String endTime, String id) {
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = UNMARK_BACKWARD_REALTIME_INDICATORS_BY_INTERVAL.replace("@", beginTime).replace("#", endTime).replace("$", id);
			statement.execute(sql);
			
			DBConnector.closeConn(conn, statement, null);
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

	public void addAbnormalRecord(AbnormalTrafficRecord r) {

		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append(ADD_ABNORMAL_RECORD + "(")
			  .append("'" + r.getId() + "',")
			  .append("'" + r.getStartTime() + "',")
			  .append("'" + r.getEndTime() + "',")
			  .append("'" + r.getDuration() + "',")
			  .append(r.getStartPointDeviationRatio() + ",")
			  .append(r.getEndPointDeviationRatio() + ",")
			  .append("'" + r.getPriority() + "',")
			  .append(r.getMaxBps() + ",")
			  .append(r.getMinBps() + ",")
			  .append(r.getAvgBps() + ",")
			  .append(r.getOutterIpNum() + ",")
			  .append("'" + r.getTop10OutterIps() + "',")
			  .append("'" + r.getOutterIpRegions() + "',")
			  .append(r.getHistoryAccessorRatio() + ",")
			  .append(r.getMaxRecordCreationNum() + ",")
			  .append(r.getMinRecordCreationNum() + ",")
			  .append(r.getAvgRecordCreationNum() + ",")
			  .append(r.getMaxBpp() + ",")
			  .append(r.getMinBpp() + ",")
			  .append(r.getAvgBpp() + ",")
			  .append(r.getMaxPps() + ",")
			  .append(r.getMinPps() + ",")
			  .append(r.getAvgPps() + ",")
			  .append(r.getMaxPpf() + ",")
			  .append(r.getMinPpf() + ",")
			  .append(r.getAvgPpf() + ",")
			  .append(r.getMaxBpf() + ",")
			  .append(r.getMinBpf() + ",")
			  .append(r.getAvgBpf() + ",")
			  .append(r.getPacketsLessThan500() + ",")
			  .append(r.getPakcetsLessThan1000() + ",")
			  .append(r.getPacketsLessThan2000() + ",")
			  .append(r.getPacketsLargeThan2000() + ",")
			  .append(r.getBytesLessThan20000() + ",")
			  .append(r.getBytesLessThan100000() + ",")
			  .append(r.getBytesLargeThan100000() + ",")
			  .append(r.getDurationLessThan1() + ",")
			  .append(r.getDurationLessThan10() + ",")
			  .append(r.getDurationLargeThan10() + ")");
			
			
			statement.execute(sb.toString());
			DBConnector.closeConn(conn, statement, null);
			  
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public List<NetflowRecord> getBackwardRawRecordByInterval(String beginTime, String endTime) {
		List<NetflowRecord> ret = new ArrayList<NetflowRecord>();
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = LOAD_BACKWARD_RAW_RECORDS_BY_INTERVAL.replace("@", beginTime).replace("#", endTime);
			ResultSet rs = statement.executeQuery(sql);
			
			while(rs.next()){
				NetflowRecord r = new NetflowRecord(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getTime(2)),
						rs.getDouble(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), 
						rs.getString(8), rs.getString(9), rs.getString(10), rs.getInt(11), (long)rs.getInt(12),
						rs.getInt(13), (long)rs.getInt(14), (long)rs.getInt(15), rs.getInt(16));
				
				ret.add(r);
			}
			DBConnector.closeConn(conn, statement, rs);
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		return ret;
	}

	@Override
	public List<RealtimeIndicatorsVO> getBackwardRealtimeIndicatorsByInterval(String beginTime, String endTime) {
		List<RealtimeIndicatorsVO> ret = new ArrayList<RealtimeIndicatorsVO>();
		
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			String sql = LOAD_BACKWARD_ABNORMAL_INDICATORS_BY_INTERVAL.replace("@", beginTime).replace("#", endTime);
			ResultSet rs = statement.executeQuery(sql);
			
			while(rs.next()){
				RealtimeIndicatorsVO vo = new RealtimeIndicatorsVO(rs.getInt(1), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getTime(2)), rs.getDouble(3), 
						rs.getDouble(6), rs.getInt(9),rs.getInt(10),rs.getInt(11),rs.getInt(12),rs.getInt(13),
						rs.getInt(14),rs.getInt(15),rs.getInt(16),rs.getInt(17),rs.getInt(18),rs.getInt(19),rs.getInt(20),
						rs.getInt(21),rs.getInt(22),rs.getInt(23));
				
				ret.add(vo);
			}
			
			DBConnector.closeConn(conn, statement, rs);
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
		return ret;
	}

	@Override
	public void doClean(String sql) {
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			statement.execute(sql);
			
			DBConnector.closeConn(conn, statement, null);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	/*@Override
	public void addTmpTrafficRecord(AbnormalTrafficRecord r) {
		try {
			Connection conn = DBConnector.getConn();
			Statement statement = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append(ADD_TMP_TRAFFIC_RECORD + "(")
			  .append("'" + r.getStartTime() + "',")
			  .append("'" + r.getEndTime() + "',")
			  .append("'" + r.getDuration() + "',")
			  .append(r.getStartPointDeviationRatio() + ",")
			  .append(r.getEndPointDeviationRatio() + ",")
			  .append("'" + r.getPriority() + "',")
			  .append(r.getMaxBps() + ",")
			  .append(r.getMinBps() + ",")
			  .append(r.getAvgBps() + ",")
			  .append(r.getOutterIpNum() + ",")
			  .append("'" + r.getTop10OutterIps() + "',")
			  .append("'" + r.getOutterIpRegions() + "',")
			  .append(r.getHistoryAccessorRatio() + ",")
			  .append(r.getMaxRecordCreationNum() + ",")
			  .append(r.getMinRecordCreationNum() + ",")
			  .append(r.getAvgRecordCreationNum() + ",")
			  .append(r.getMaxBpp() + ",")
			  .append(r.getMinBpp() + ",")
			  .append(r.getAvgBpp() + ",")
			  .append(r.getMaxPps() + ",")
			  .append(r.getMinPps() + ",")
			  .append(r.getAvgPps() + ",")
			  .append(r.getMaxPpf() + ",")
			  .append(r.getMinPpf() + ",")
			  .append(r.getAvgPpf() + ",")
			  .append(r.getMaxBpf() + ",")
			  .append(r.getMinBpf() + ",")
			  .append(r.getAvgBpf() + ",")
			  .append(r.getPacketsLessThan500() + ",")
			  .append(r.getPakcetsLessThan1000() + ",")
			  .append(r.getPacketsLessThan2000() + ",")
			  .append(r.getPacketsLargeThan2000() + ",")
			  .append(r.getBytesLessThan20000() + ",")
			  .append(r.getBytesLessThan100000() + ",")
			  .append(r.getBytesLargeThan100000() + ",")
			  .append(r.getDurationLessThan1() + ",")
			  .append(r.getDurationLessThan10() + ",")
			  .append(r.getDurationLargeThan10() + ")");
			
			
			statement.execute(sb.toString());
			DBConnector.closeConn(conn, statement, null);
			  
		} catch (Exception e) {
			// TODO: handle exception
		}
	}*/

}
