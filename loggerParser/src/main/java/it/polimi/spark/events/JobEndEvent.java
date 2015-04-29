package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class JobEndEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerJobEnd";
	private long completionTime;
	private int jobID;

	private String jobResult;

	public long getCompletionTime() {
		return completionTime;
	}

	public int getJobID() {
		return jobID;
	}

	public String getJobResult() {
		return jobResult;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JobEndEvent initialize(JsonObject jsonObject) {
		jobID = jsonObject.get("Job ID").getAsInt();
		completionTime = jsonObject.get("Completion Time").getAsLong();
		jobResult = jsonObject.get("Job Result").getAsJsonObject().get("Result").getAsString();
		return this;
	}

	public void setCompletionTime(long completionTime) {
		this.completionTime = completionTime;
	}

	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	public void setJobResult(String jobResult) {
		this.jobResult = jobResult;
	}
	

}
