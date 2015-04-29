package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class ExecutorInfo implements JsonInitializable {
	private String host;
	private String stderrUrl;
	private String stdoutUrl;
	private int totalCores;

	public String getHost() {
		return host;
	}

	public String getStderrUrl() {
		return stderrUrl;
	}

	public String getStdoutUrl() {
		return stdoutUrl;
	}

	public int getTotalCores() {
		return totalCores;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ExecutorInfo initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		host = jsonObject.get("Host").getAsString();
		totalCores = jsonObject.get("Total Cores").getAsInt();
		stdoutUrl = jsonObject.get("Log Urls").getAsJsonObject().get("stdout")
				.getAsString();
		stderrUrl = jsonObject.get("Log Urls").getAsJsonObject().get("stderr")
				.getAsString();
		return this;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setStderrUrl(String stderrUrl) {
		this.stderrUrl = stderrUrl;
	}

	public void setStdoutUrl(String stdoutUrl) {
		this.stdoutUrl = stdoutUrl;
	}

	public void setTotalCores(int totalCores) {
		this.totalCores = totalCores;
	}
}
