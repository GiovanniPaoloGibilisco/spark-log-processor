package it.polimi.spark.estimator;

import java.text.DecimalFormat;

public class EstimationResult {

	private String appID;
	private double appSize;
	private long realDuration;
	private long estimatedDuration;
	private long error;
	private double relativeError;
	
	
	
	public EstimationResult(String appID, double appSize, long realDuration,
			long estimatedDuration) {
		super();
		this.appID = appID;
		this.appSize = appSize;
		this.realDuration = realDuration;
		this.estimatedDuration = estimatedDuration;
		
		DecimalFormat df = new DecimalFormat("#####.##");      
		this.appSize = Double.valueOf(df.format(appSize));
		
		error = Math.abs(realDuration-estimatedDuration);
		relativeError = ((double) error)/((double) realDuration) * 100;
		df = new DecimalFormat("##.#");      
		relativeError = Double.valueOf(df.format(relativeError));
	}
	public String getAppID() {
		return appID;
	}
	public double getAppSize() {
		return appSize;
	}
	public long getRealDuration() {
		return realDuration;
	}
	public long getEstimatedDuration() {
		return estimatedDuration;
	}
	public long getError() {
		return error;
	}
	public double getRelativeError() {
		return relativeError;
	}
	
	
}
