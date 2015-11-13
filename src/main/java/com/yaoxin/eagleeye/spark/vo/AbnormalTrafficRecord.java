package com.yaoxin.eagleeye.spark.vo;

import java.io.Serializable;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月7日
 */
public class AbnormalTrafficRecord implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -909825676000037274L;
	
	private String id;
	private String startTime ;
	private String endTime ;
	private String duration ;
	private double startPointDeviationRatio ;
	private double endPointDeviationRatio ;
	private String priority ;
	private double maxBps ;
	private double minBps ;
	private double avgBps ;
	private Long outterIpNum ;
	private String top10OutterIps ;
	private String outterIpRegions ;
	private double historyAccessorRatio ;
	private int maxRecordCreationNum ;
	private int minRecordCreationNum ;
	private int avgRecordCreationNum ;
	private int maxBpp ;
	private int minBpp ;
	private int avgBpp ;
	private int maxPps ;
	private int minPps ;
	private int avgPps ;
	private int maxPpf ;
	private int minPpf ;
	private int avgPpf ;
	private int maxBpf ;
	private int minBpf ;
	private int avgBpf ;
	private int packetsLessThan500 ;
	private int pakcetsLessThan1000 ;
	private int packetsLessThan2000 ;
	private int packetsLargeThan2000 ;
	private int bytesLessThan20000 ;
	private int bytesLessThan100000 ;
	private int bytesLargeThan100000 ;
	private int durationLessThan1 ;
	private int durationLessThan10 ;
	private int durationLargeThan10 ;
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getDuration() {
		return duration;
	}
	public void setDuration(String duration) {
		this.duration = duration;
	}
	public double getStartPointDeviationRatio() {
		return startPointDeviationRatio;
	}
	public void setStartPointDeviationRatio(double startPointDeviationRatio) {
		this.startPointDeviationRatio = startPointDeviationRatio;
	}
	public double getEndPointDeviationRatio() {
		return endPointDeviationRatio;
	}
	public void setEndPointDeviationRatio(double endPointDeviationRatio) {
		this.endPointDeviationRatio = endPointDeviationRatio;
	}
	public String getPriority() {
		return priority;
	}
	public void setPriority(String priority) {
		this.priority = priority;
	}
	public double getMaxBps() {
		return maxBps;
	}
	public void setMaxBps(double maxBps) {
		this.maxBps = maxBps;
	}
	public double getMinBps() {
		return minBps;
	}
	public void setMinBps(double minBps) {
		this.minBps = minBps;
	}
	public double getAvgBps() {
		return avgBps;
	}
	public void setAvgBps(double avgBps) {
		this.avgBps = avgBps;
	}
	public long getOutterIpNum() {
		return outterIpNum;
	}
	public void setOutterIpNum(Long numOfOutterIps) {
		this.outterIpNum = numOfOutterIps;
	}
	public String getTop10OutterIps() {
		return top10OutterIps;
	}
	public void setTop10OutterIps(String top10OutterIps) {
		this.top10OutterIps = top10OutterIps;
	}
	public String getOutterIpRegions() {
		return outterIpRegions;
	}
	public void setOutterIpRegions(String outterIpRegions) {
		this.outterIpRegions = outterIpRegions;
	}
	public double getHistoryAccessorRatio() {
		return historyAccessorRatio;
	}
	public void setHistoryAccessorRatio(double historyAccessorRatio) {
		this.historyAccessorRatio = historyAccessorRatio;
	}
	public int getMaxRecordCreationNum() {
		return maxRecordCreationNum;
	}
	public void setMaxRecordCreationNum(int maxRecordCreationNum) {
		this.maxRecordCreationNum = maxRecordCreationNum;
	}
	public int getMinRecordCreationNum() {
		return minRecordCreationNum;
	}
	public void setMinRecordCreationNum(int minRecordCreationNum) {
		this.minRecordCreationNum = minRecordCreationNum;
	}
	public int getAvgRecordCreationNum() {
		return avgRecordCreationNum;
	}
	public void setAvgRecordCreationNum(int avgRecordCreationNum) {
		this.avgRecordCreationNum = avgRecordCreationNum;
	}
	public int getMaxBpp() {
		return maxBpp;
	}
	public void setMaxBpp(int maxBpp) {
		this.maxBpp = maxBpp;
	}
	public int getMinBpp() {
		return minBpp;
	}
	public void setMinBpp(int minBpp) {
		this.minBpp = minBpp;
	}
	public int getAvgBpp() {
		return avgBpp;
	}
	public void setAvgBpp(int avgBpp) {
		this.avgBpp = avgBpp;
	}
	public int getMaxPps() {
		return maxPps;
	}
	public void setMaxPps(int maxPps) {
		this.maxPps = maxPps;
	}
	public int getMinPps() {
		return minPps;
	}
	public void setMinPps(int minPps) {
		this.minPps = minPps;
	}
	public int getAvgPps() {
		return avgPps;
	}
	public void setAvgPps(int avgPps) {
		this.avgPps = avgPps;
	}
	public int getMaxPpf() {
		return maxPpf;
	}
	public void setMaxPpf(int maxPpf) {
		this.maxPpf = maxPpf;
	}
	public int getMinPpf() {
		return minPpf;
	}
	public void setMinPpf(int minPpf) {
		this.minPpf = minPpf;
	}
	public int getAvgPpf() {
		return avgPpf;
	}
	public void setAvgPpf(int avgPpf) {
		this.avgPpf = avgPpf;
	}
	public int getMaxBpf() {
		return maxBpf;
	}
	public void setMaxBpf(int maxBpf) {
		this.maxBpf = maxBpf;
	}
	public int getMinBpf() {
		return minBpf;
	}
	public void setMinBpf(int minBpf) {
		this.minBpf = minBpf;
	}
	public int getAvgBpf() {
		return avgBpf;
	}
	public void setAvgBpf(int avgBpf) {
		this.avgBpf = avgBpf;
	}
	public int getPacketsLessThan500() {
		return packetsLessThan500;
	}
	public void setPacketsLessThan500(int packetsLessThan500) {
		this.packetsLessThan500 = packetsLessThan500;
	}
	public int getPakcetsLessThan1000() {
		return pakcetsLessThan1000;
	}
	public void setPakcetsLessThan1000(int pakcetsLessThan1000) {
		this.pakcetsLessThan1000 = pakcetsLessThan1000;
	}
	public int getPacketsLessThan2000() {
		return packetsLessThan2000;
	}
	public void setPacketsLessThan2000(int packetsLessThan2000) {
		this.packetsLessThan2000 = packetsLessThan2000;
	}
	public int getPacketsLargeThan2000() {
		return packetsLargeThan2000;
	}
	public void setPacketsLargeThan2000(int packetsLargeThan2000) {
		this.packetsLargeThan2000 = packetsLargeThan2000;
	}
	public int getBytesLessThan20000() {
		return bytesLessThan20000;
	}
	public void setBytesLessThan20000(int bytesLessThan20000) {
		this.bytesLessThan20000 = bytesLessThan20000;
	}
	public int getBytesLessThan100000() {
		return bytesLessThan100000;
	}
	public void setBytesLessThan100000(int bytesLessThan100000) {
		this.bytesLessThan100000 = bytesLessThan100000;
	}
	public int getBytesLargeThan100000() {
		return bytesLargeThan100000;
	}
	public void setBytesLargeThan100000(int bytesLargeThan100000) {
		this.bytesLargeThan100000 = bytesLargeThan100000;
	}
	public int getDurationLessThan1() {
		return durationLessThan1;
	}
	public void setDurationLessThan1(int durationLessThan1) {
		this.durationLessThan1 = durationLessThan1;
	}
	public int getDurationLessThan10() {
		return durationLessThan10;
	}
	public void setDurationLessThan10(int durationLessThan10) {
		this.durationLessThan10 = durationLessThan10;
	}
	public int getDurationLargeThan10() {
		return durationLargeThan10;
	}
	public void setDurationLargeThan10(int durationLargeThan10) {
		this.durationLargeThan10 = durationLargeThan10;
	}
}
