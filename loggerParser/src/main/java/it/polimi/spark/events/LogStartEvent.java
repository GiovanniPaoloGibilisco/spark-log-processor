package it.polimi.spark.events;

public class LogStartEvent extends SparkListenerEvent {
	private String sparkVersion;

	public String getSparkVersion() {
		return sparkVersion;
	}

	public void setSparkVersion(String sparkVersion) {
		this.sparkVersion = sparkVersion;
	}
}
