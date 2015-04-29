package it.polimi.spark.events;

import java.util.List;

public class StageInfo {
	private int stageID;
	private int stageAttemptID;
	private String stageName;
	private int numberOfTasks;
	private String details;
	private Long sumbissionTime;
	private List<RDDInfo> rddInfo;

	//TODO: find out what the Accumulable tag is used for in the log file. 
	public int getStageID() {
		return stageID;
	}

	public void setStageID(int stageID) {
		this.stageID = stageID;
	}

	public int getStageAttemptID() {
		return stageAttemptID;
	}

	public void setStageAttemptID(int stageAttemptID) {
		this.stageAttemptID = stageAttemptID;
	}

	public String getStageName() {
		return stageName;
	}

	public void setStageName(String stageName) {
		this.stageName = stageName;
	}

	public int getNumberOfTasks() {
		return numberOfTasks;
	}

	public void setNumberOfTasks(int numberOfTasks) {
		this.numberOfTasks = numberOfTasks;
	}

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

	public Long getSumbissionTime() {
		return sumbissionTime;
	}

	public void setSumbissionTime(Long sumbissionTime) {
		this.sumbissionTime = sumbissionTime;
	}

	public List<RDDInfo> getRddInfo() {
		return rddInfo;
	}

	public void setRddInfo(List<RDDInfo> rddInfo) {
		this.rddInfo = rddInfo;
	}

}
