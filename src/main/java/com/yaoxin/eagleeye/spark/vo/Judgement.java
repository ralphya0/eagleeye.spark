package com.yaoxin.eagleeye.spark.vo;

import java.util.Map;

/**
 * 
 * @author yaoxin   
 * 
 * 2015年11月6日
 */
public class Judgement {

	private Double deviationRatio;
	private Direction direction;
	private Map<String, Double> keyIndicatorDeviationRatios;
	private Integer priority;
	
	public Integer getPriority() {
		return priority;
	}
	public void setPriority(Integer priority) {
		this.priority = priority;
	}
	public Double getDeviationRatio() {
		return deviationRatio;
	}
	public void setDeviationRatio(Double deviationRatio) {
		this.deviationRatio = deviationRatio;
	}
	public Direction getDirection() {
		return direction;
	}
	public void setDirection(Direction direction) {
		this.direction = direction;
	}
	public Map<String, Double> getKeyIndicatorDeviationRatios() {
		return keyIndicatorDeviationRatios;
	}
	public void setKeyIndicatorDeviationRatios(Map<String, Double> keyIndicatorDeviationRatios) {
		this.keyIndicatorDeviationRatios = keyIndicatorDeviationRatios;
	}
	
	
}
