package com.yaoxin.eagleeye.spark.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.yaoxin.eagleeye.spark.storage.StorageManager;
import com.yaoxin.eagleeye.spark.storage.impl.MysqlStorageManager;
import com.yaoxin.eagleeye.spark.util.BpfComparator;
import com.yaoxin.eagleeye.spark.util.BppComparator;
import com.yaoxin.eagleeye.spark.util.BpsComparator;
import com.yaoxin.eagleeye.spark.util.NetflowRecordComparator;
import com.yaoxin.eagleeye.spark.util.PpfComparator;
import com.yaoxin.eagleeye.spark.util.PpsComparator;
import com.yaoxin.eagleeye.spark.util.RecordCreationComparator;
import com.yaoxin.eagleeye.spark.vo.AbnormalIntervalEdgeType;
import com.yaoxin.eagleeye.spark.vo.AbnormalTrafficRecord;
import com.yaoxin.eagleeye.spark.vo.Direction;
import com.yaoxin.eagleeye.spark.vo.Judgement;
import com.yaoxin.eagleeye.spark.vo.NetflowRecord;
import com.yaoxin.eagleeye.spark.vo.RealtimeIndicatorsVO;
import com.yaoxin.eagleeye.spark.vo.SuspiciousAbnormalIntervalEdge;

import scala.Tuple2;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public class AbnormalIntervalMatcher {
	
	public final static double START_POINT_THRESHOLD = 1.0;
	public final static double END_POINT_THRESHOLD = 1.0;
	public final static String ABNORMAL_RECORD_ID_PREFIX = "abnormal.";
	public final static int LONG_INTERVAL_THRESHOLD = 3;
	
	private StorageManager storageManager = new MysqlStorageManager();
	
	private AbnormalIntervalEdgeDetector edgeDetector = new AbnormalIntervalEdgeDetector();
	private SuspiciousAbnormalIntervalEdge latestStartEdge; // 缓存可疑异常区间的起始端点(对该变量的最新策略如下)
	private SuspiciousAbnormalIntervalEdge tmpEndEdge;  // 缓存下降幅度最大的端点(存在可疑左端点的情况下), 防止出现时间跨度过长的异常区间
	private Date lastTimeWindow;
	private String currentAbnormalId ;
	private boolean forceOutputInterval = false;

	
	public void schedule(JavaSparkContext ctx){
		RealtimeIndicatorsVO vo = this.storageManager.getRealtimeIndicator();
		
		if(vo != null && isNewItem(vo)){
			
			trunckLongInterval(vo.getTimeFrame());

			// 判断是否为异常区间的端点
			Judgement result = this.edgeDetector.doJudge(vo);
			
			if(this.forceOutputInterval){
				processAbnormalInterval(result, ctx);
				
				cleanupStatus();
			}
			else if(result.getDirection() == Direction.LOW_2_HIGH){
				
				if(result.getDeviationRatio() >= START_POINT_THRESHOLD){
					updateStartEdgeIfNecessary(result, vo);
				}
				
				if(this.latestStartEdge != null){
					// 若存在左端点, 则对当前time frame进行标记, 将其原始netflow数据和indicators都存下来
					this.storageManager.markRawRecord(vo.getTimeFrame(), this.currentAbnormalId);
					this.storageManager.markRealtimeIndicators(vo.getTimeFrame(), this.currentAbnormalId);
					
				}
			}
			else if(result.getDirection() == Direction.HIGH_2_LOW){
				if(this.latestStartEdge != null){
					// 若存在左端点, 则对当前time frame进行标记, 将其原始netflow数据和indicators都存下来
					this.storageManager.markRawRecord(vo.getTimeFrame(), this.currentAbnormalId);
					this.storageManager.markRealtimeIndicators(vo.getTimeFrame(), this.currentAbnormalId);
				}
				
				if(this.tmpEndEdge == null || this.tmpEndEdge.getJudgement().getPriority() < result.getPriority()){
					SuspiciousAbnormalIntervalEdge newEnd = new SuspiciousAbnormalIntervalEdge();
					newEnd.setTimeFrame(vo.getTimeFrame());
					newEnd.setEdgeType(AbnormalIntervalEdgeType.END_POINT);
					newEnd.setIndicators(vo);
					newEnd.setJudgement(result);
					
					this.tmpEndEdge = newEnd;
				}	
				
				if(result.getDeviationRatio() >= END_POINT_THRESHOLD){
					processAbnormalInterval(result, ctx);
					
					cleanupStatus();
				}

			}
			
			processHalfHourIntervalIfNecessary(result, ctx);
		}
	}
	
	void updateStartEdgeIfNecessary(Judgement judgement, RealtimeIndicatorsVO vo){
		
		try {
			if(this.latestStartEdge == null 
					|| this.latestStartEdge.getJudgement().getPriority() < judgement.getPriority()){
				
				if(this.currentAbnormalId != null && this.latestStartEdge != null){
					this.storageManager.unmarkRawRecordById(currentAbnormalId);
					this.storageManager.unmarkRealtimeIndicatorsById(currentAbnormalId);
				}
				
				SuspiciousAbnormalIntervalEdge edge = new SuspiciousAbnormalIntervalEdge();
				edge.setTimeFrame(vo.getTimeFrame());
				edge.setEdgeType(AbnormalIntervalEdgeType.START_POINT);
				edge.setIndicators(vo);
				edge.setJudgement(judgement);
				
				// 将当前time frame标记为异常起始端点
				this.latestStartEdge = edge;
				Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(edge.getTimeFrame());
				this.currentAbnormalId = ABNORMAL_RECORD_ID_PREFIX + new SimpleDateFormat("yyyyMMddHHmm").format(tmp);
			}
		} catch (ParseException e) {
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
		
	}
	
	void processAbnormalInterval(Judgement judgement, JavaSparkContext ctx){
		if(this.latestStartEdge != null && this.currentAbnormalId != null){
			// 将所有数据从mysql中读取出来然后调用parallelize转化成RDD, 这种方案的潜在问题是数据量较大时一次性
			// 把所有原始数据加载到内存之后可能会引起OOM异常... 
			// 所以必要时可以把相关数据写入HDFS或local file system中, 然后指定HDFS为Spark的数据源 
			// or 设置worker内存大小和jvm参数来避免OOM
			List<NetflowRecord> rawRecords = this.storageManager.getAbnormalRawRecord(currentAbnormalId);
			List<RealtimeIndicatorsVO> vos = this.storageManager.getAbnormalRealtimeIndicators(currentAbnormalId);
			
			calculateAndSaveResult(rawRecords, vos, judgement, ctx);
		}
	}
	
	
	void processHalfHourIntervalIfNecessary(Judgement judgement, JavaSparkContext ctx){
		try {
			String now = new SimpleDateFormat("HH:mm").format(lastTimeWindow);
			
			if(now.endsWith("00") || now.endsWith("30")){
				
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(lastTimeWindow);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				
				String endTime = sdf.format(calendar.getTime());
				
				calendar.add(Calendar.MINUTE, -30);
				
				String beginTime = sdf.format(calendar.getTime()); 
				
				List<NetflowRecord> rawRecords = this.storageManager.getRawRecordByInterval(beginTime, endTime);
				List<RealtimeIndicatorsVO> vos = this.storageManager.getRealtimeIndicatorsByInterval(beginTime, endTime);
				
				calculateAndSaveResult(rawRecords, vos, judgement, ctx);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	void calculateAndSaveResult(List<NetflowRecord> r, List<RealtimeIndicatorsVO> v, Judgement judgement, JavaSparkContext ctx){
		try {
			
			JavaRDD<NetflowRecord> raw = ctx.parallelize(r);
			JavaRDD<String> outterIps = raw.map(new Function<NetflowRecord, String>(){

				public String call(NetflowRecord arg0) throws Exception {
					
					return arg0.getDstIp();
				}
				
			}).distinct();
			
			Long numOfOutterIps = outterIps.count();
			
			JavaPairRDD<String, NetflowRecord> outterIpAsKey = 
					raw.mapToPair(new PairFunction<NetflowRecord, String, NetflowRecord>(){
						public Tuple2<String, NetflowRecord> call(NetflowRecord arg0) throws Exception {
							
							return new Tuple2<String, NetflowRecord>(arg0.getDstIp(), arg0);
						}
			});
			
			// 统计outter ip top 10
			JavaPairRDD<String, NetflowRecord> reduced = outterIpAsKey.reduceByKey(
					new Function2<NetflowRecord, NetflowRecord, NetflowRecord>(){

						public NetflowRecord call(NetflowRecord arg0, NetflowRecord arg1) throws Exception {
							arg0.setBytes(arg0.getBytes() + arg1.getBytes());
							return arg0;
						}
			});
			
			List<Tuple2<String, NetflowRecord>> top10 = reduced.top(10, new NetflowRecordComparator());
			
			JavaRDD<RealtimeIndicatorsVO> indicators = ctx.parallelize(v);
			
			RealtimeIndicatorsVO maxBps = indicators.max(new BpsComparator());
			RealtimeIndicatorsVO minBps = indicators.min(new BpsComparator());
			RealtimeIndicatorsVO maxCreation = indicators.max(new RecordCreationComparator());
			RealtimeIndicatorsVO minCreation = indicators.min(new RecordCreationComparator());
			RealtimeIndicatorsVO maxBpp = indicators.max(new BppComparator());
			RealtimeIndicatorsVO minBpp = indicators.min(new BppComparator());
			RealtimeIndicatorsVO maxPps = indicators.max(new PpsComparator());
			RealtimeIndicatorsVO minPps = indicators.min(new PpsComparator());
			RealtimeIndicatorsVO maxPpf = indicators.max(new PpfComparator());
			RealtimeIndicatorsVO minPpf = indicators.min(new PpfComparator());
			RealtimeIndicatorsVO maxBpf = indicators.max(new BpfComparator());
			RealtimeIndicatorsVO minBpf = indicators.min(new BpfComparator());
			
			RealtimeIndicatorsVO totalBps = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

				public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
						throws Exception {
					arg0.setBps(arg0.getBps() + arg1.getBps());
					return arg0;
				}
				
			});
			
			RealtimeIndicatorsVO totalCreation = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

						public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
								throws Exception {
							arg0.setRecordCreationNum(arg0.getRecordCreationNum() + arg1.getRecordCreationNum());
							return arg0;
						}
						
					});
			
			RealtimeIndicatorsVO totalBpp = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

						public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
								throws Exception {
							arg0.setBpp(arg0.getBpp() + arg1.getBpp());
							return arg0;
						}
						
					});
			
			RealtimeIndicatorsVO totalPps = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

						public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
								throws Exception {
							arg0.setPps(arg0.getPps() + arg1.getPps());
							return arg0;
						}
						
					});
			
			RealtimeIndicatorsVO totalPpf = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

						public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
								throws Exception {
							arg0.setPpf(arg0.getPpf() + arg1.getPpf());
							return arg0;
						}
						
					});
			
			RealtimeIndicatorsVO totalBpf = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

						public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
								throws Exception {
							arg0.setBpf(arg0.getBpf() + arg1.getBpf());
							return arg0;
						}
						
					});
			
			
			long numOfIndicator = indicators.count();
			
			RealtimeIndicatorsVO interval = 
					indicators.reduce(new Function2<RealtimeIndicatorsVO, RealtimeIndicatorsVO, RealtimeIndicatorsVO>(){

				public RealtimeIndicatorsVO call(RealtimeIndicatorsVO arg0, RealtimeIndicatorsVO arg1)
						throws Exception {
					arg0.setPacketsLessThan500(arg0.getPacketsLessThan500() + arg1.getPacketsLessThan500());
					arg0.setPakcetsLessThan1000(arg0.getPakcetsLessThan1000() + arg1.getPakcetsLessThan1000());
					arg0.setPacketsLessThan2000(arg0.getPacketsLessThan2000() + arg1.getPacketsLessThan2000());
					arg0.setPacketsLargeThan2000(arg0.getPacketsLargeThan2000() + arg1.getPacketsLargeThan2000());
					arg0.setBytesLessThan20000(arg0.getBytesLessThan20000() + arg1.getBytesLessThan20000());
					arg0.setBytesLessThan100000(arg0.getBytesLessThan100000() + arg1.getBytesLessThan100000());
					arg0.setBytesLargeThan100000(arg0.getBytesLargeThan100000() + arg1.getBytesLargeThan100000());
					arg0.setDurationLessThan1(arg0.getDurationLessThan1() + arg1.getDurationLessThan1());
					arg0.setDurationLessThan10(arg0.getDurationLessThan10() + arg1.getDurationLessThan10());
					arg0.setDurationLargeThan10(arg0.getDurationLargeThan10() + arg1.getDurationLargeThan10());
					
					return arg0;
				}
			});
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			AbnormalTrafficRecord record = new AbnormalTrafficRecord();
			record.setId(currentAbnormalId);
			record.setStartTime(this.latestStartEdge.getTimeFrame());
			
			if(this.forceOutputInterval){
				record.setEndTime(this.tmpEndEdge.getTimeFrame());
				long diff = sdf.parse(this.tmpEndEdge.getTimeFrame()).getTime() - sdf.parse(this.latestStartEdge.getTimeFrame()).getTime();
				double secs = diff / 1000;
				record.setDuration(secs + "s");
				record.setEndPointDeviationRatio(this.tmpEndEdge.getJudgement().getDeviationRatio());
			}
			else{
				record.setEndTime(sdf.format(this.lastTimeWindow));
				long diff = this.lastTimeWindow.getTime() - sdf.parse(this.latestStartEdge.getTimeFrame()).getTime();
				double secs = diff / 1000;
				record.setDuration(secs + "s");
				record.setEndPointDeviationRatio(judgement.getDeviationRatio());
			}
			
			record.setStartPointDeviationRatio(this.latestStartEdge.getJudgement().getDeviationRatio());
			record.setPriority("xxxxx");
			record.setOutterIpNum(numOfOutterIps);
			record.setOutterIpRegions("xxxxxx");
			StringBuilder sb1 = new StringBuilder();
			for(Tuple2<String, NetflowRecord> e : top10){
				sb1.append(e._1 + "#" + e._2.getBytes() + ",");
			}
			if(sb1.lastIndexOf(",") == sb1.length() - 1)
				sb1.deleteCharAt(sb1.length() - 1);
			
			record.setTop10OutterIps(sb1.toString());
			record.setMaxBps(maxBps.getBps());
			record.setMinBps(minBps.getBps());
			record.setMaxRecordCreationNum(maxCreation.getRecordCreationNum());
			record.setMinRecordCreationNum(minCreation.getRecordCreationNum());
			record.setMaxBpp(maxBpp.getBpp());
			record.setMinBpp(minBpp.getBpp());
			record.setMaxPps(maxPps.getPps());
			record.setMinPps(minPps.getPps());
			record.setMaxPpf(maxPpf.getPpf());
			record.setMinPpf(minPpf.getPpf());
			record.setMaxBpf(maxBpf.getBpf());
			record.setMinBpf(minBpf.getBpf());
			record.setAvgBps(totalBps.getBps() / numOfIndicator);
			record.setAvgRecordCreationNum((int) (totalCreation.getRecordCreationNum() / numOfIndicator));
			record.setAvgBpp((int) (totalBpp.getBpp() / numOfIndicator));
			record.setAvgPps((int) (totalPps.getPps() / numOfIndicator));
			record.setAvgPpf((int) (totalPpf.getPpf() / numOfIndicator));
			record.setAvgBpf((int) (totalBpf.getBpf() / numOfIndicator));
			record.setPacketsLessThan500(interval.getPacketsLessThan500());
			record.setPakcetsLessThan1000(interval.getPakcetsLessThan1000());
			record.setPacketsLessThan2000(interval.getPacketsLessThan2000());
			record.setPacketsLargeThan2000(interval.getPacketsLargeThan2000());
			record.setBytesLessThan20000(interval.getBytesLessThan20000());
			record.setBytesLessThan100000(interval.getBytesLessThan100000());
			record.setBytesLargeThan100000(interval.getBytesLargeThan100000());
			record.setDurationLessThan1(interval.getDurationLessThan1());
			record.setDurationLessThan10(interval.getDurationLessThan10());
			record.setDurationLargeThan10(interval.getDurationLargeThan10());
			
			// 将结果写入mysql
			this.storageManager.addAbnormalRecord(record);
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
	}
	
	boolean isNewItem(RealtimeIndicatorsVO vo){
		try {
			
			Date rc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(vo.getTimeFrame());

			// 丢弃所有旧数据
			if(this.lastTimeWindow == null || this.lastTimeWindow.before(rc)){
				
				long diff = rc.getTime() - this.lastTimeWindow.getTime();
				double minutes = diff / 60000;
				if(minutes >= 15){
					// 如果网络不稳定造成15分钟以上的数据丢失现象, 那么强制更新缓存, 丢弃之前的状态
					this.currentAbnormalId = null;
					this.latestStartEdge = null;
				}
				
				this.lastTimeWindow = rc;
				return true;
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	
	void trunckLongInterval(String timeFrame){
		try {
			
			if(this.latestStartEdge != null){
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				Date now = sdf.parse(timeFrame);
				Date begin = sdf.parse(this.latestStartEdge.getTimeFrame());
				long diff = now.getTime() - begin.getTime();
				double hours = diff / 3600000;
				
				// 强制关闭长度超过LONG_INTERVAL_THRESHOLD小时的异常区间
				if(hours > LONG_INTERVAL_THRESHOLD && this.tmpEndEdge != null){
					this.forceOutputInterval = true;

					// 清除标记
					Calendar startTime = Calendar.getInstance();
					startTime.setTime(sdf.parse(this.tmpEndEdge.getTimeFrame()));
					startTime.add(Calendar.MINUTE, 1);
					
					Calendar endTime = Calendar.getInstance();
					endTime.setTime(now);
					endTime.add(Calendar.MINUTE, 1);
					
					this.storageManager.unmarkRawRecordByInterval(sdf.format(startTime.getTime()), 
							sdf.format(endTime.getTime()), currentAbnormalId);
					this.storageManager.unmarkRealtimeIndicatorsByInterval(sdf.format(startTime.getTime()), 
							sdf.format(endTime.getTime()), currentAbnormalId);
				}
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	void cleanupStatus(){
		this.forceOutputInterval = false;
		this.tmpEndEdge = null;
		this.currentAbnormalId = null;
		this.latestStartEdge = null;
	}
}
