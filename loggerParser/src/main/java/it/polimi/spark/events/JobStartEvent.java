package it.polimi.spark.events;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class JobStartEvent extends SparkListenerEvent{
	static final String eventTag = "SparkListenerJobStart";
	private int jobID;
	private List<Integer> stageIDs;
	private List<StageInfo> stageInfos;

	private long submissionTime;

	public int getJobID() {
		return jobID;
	}

	public List<Integer> getStageIDs() {
		return stageIDs;
	}

	public List<StageInfo> getStageInfos() {
		return stageInfos;
	}

	public long getSubmissionTime() {
		return submissionTime;
	}

	@SuppressWarnings("unchecked")
	@Override
	public JobStartEvent initialize(JsonObject jsonObject) {
		jobID = jsonObject.get("Job ID").getAsInt();
		submissionTime = jsonObject.get("Submission Time").getAsLong();
		stageIDs = new ArrayList<Integer>();
		JsonArray ids = jsonObject.get("Stage IDs").getAsJsonArray();
		for(int i=0; i<ids.size(); i++)
			stageIDs.add(ids.get(i).getAsInt());
		
		stageInfos = new ArrayList<StageInfo>();
		JsonArray infos = jsonObject.get("Stage Infos").getAsJsonArray();
		for(int i=0; i<infos.size(); i++)
			stageInfos.add(new StageInfo().initialize(infos.get(i).getAsJsonObject()));
		
		return this;
	}

	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	public void setStageIDs(List<Integer> stageIDs) {
		this.stageIDs = stageIDs;
	}

	public void setStageInfos(List<StageInfo> stageInfos) {
		this.stageInfos = stageInfos;
	}

	
	public void setSubmissionTime(long submissionTime) {
		this.submissionTime = submissionTime;
	}
	

}
