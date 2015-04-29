package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class LogStartEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerLogStart";

	private String sparkVersion;

	public String getSparkVersion() {
		return sparkVersion;
	}

	@SuppressWarnings("unchecked")
	@Override
	public LogStartEvent initialize(JsonObject jsonObject) {
		sparkVersion = jsonObject.get("Spark Version").getAsString();
		return this;
	}

	public void setSparkVersion(String sparkVersion) {
		this.sparkVersion = sparkVersion;
	}
	
}
