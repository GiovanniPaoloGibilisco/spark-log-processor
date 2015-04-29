package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class ApplicationEndEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerApplicationEnd";
	private long timestamp;

	public long getTimestamp() {
		return timestamp;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ApplicationEndEvent initialize(JsonObject jsonObject) {
		timestamp = jsonObject.get("Timestamp").getAsLong();
		return this;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
