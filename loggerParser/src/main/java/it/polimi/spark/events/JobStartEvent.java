package it.polimi.spark.events;

import java.util.List;

public class JobStartEvent {
	private int jobID;
	private long submissionTime;
	private List<StageInfo> stageInfos;
	private List<Integer> stageIDs;

	public int getJobID() {
		return jobID;
	}

	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	public long getSubmissionTime() {
		return submissionTime;
	}

	public void setSubmissionTime(long submissionTime) {
		this.submissionTime = submissionTime;
	}

	public List<StageInfo> getStageInfos() {
		return stageInfos;
	}

	public void setStageInfos(List<StageInfo> stageInfos) {
		this.stageInfos = stageInfos;
	}

	public List<Integer> getStageIDs() {
		return stageIDs;
	}

	public void setStageIDs(List<Integer> stageIDs) {
		this.stageIDs = stageIDs;
	}

}
