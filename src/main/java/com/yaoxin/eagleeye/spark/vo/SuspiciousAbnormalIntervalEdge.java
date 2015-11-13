package com.yaoxin.eagleeye.spark.vo;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public class SuspiciousAbnormalIntervalEdge {

	private String timeFrame;
	private AbnormalIntervalEdgeType edgeType;
	private RealtimeIndicatorsVO indicators;
	private Judgement judgement;
	
	
	public String getTimeFrame() {
		return timeFrame;
	}
	public void setTimeFrame(String timeFrame) {
		this.timeFrame = timeFrame;
	}
	public AbnormalIntervalEdgeType getEdgeType() {
		return edgeType;
	}
	public void setEdgeType(AbnormalIntervalEdgeType edgeType) {
		this.edgeType = edgeType;
	}
	public RealtimeIndicatorsVO getIndicators() {
		return indicators;
	}
	public void setIndicators(RealtimeIndicatorsVO indicators) {
		this.indicators = indicators;
	}
	public Judgement getJudgement() {
		return judgement;
	}
	public void setJudgement(Judgement judgement) {
		this.judgement = judgement;
	}
	
}
