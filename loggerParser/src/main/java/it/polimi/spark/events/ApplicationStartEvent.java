package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class ApplicationStartEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerApplicationStart";

	private String applicationID;
	private String applicationName;
	private Long timestamp;
	private String user;

	public String getApplicationID() {
		return applicationID;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public String getUser() {
		return user;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ApplicationStartEvent initialize(JsonObject jsonObject) {
		applicationName = jsonObject.get("App Name").getAsString();
		applicationID = jsonObject.get("App ID").getAsString();
		timestamp = jsonObject.get("Timestamp").getAsLong();
		user = jsonObject.get("User").getAsString();
		return this;
	}

	public void setApplicationID(String applicationID) {
		this.applicationID = applicationID;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public void setUser(String user) {
		this.user = user;
	}

}
