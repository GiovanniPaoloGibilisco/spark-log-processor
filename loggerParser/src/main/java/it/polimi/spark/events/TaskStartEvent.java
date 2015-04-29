package it.polimi.spark.events;

public class TaskStartEvent {

	private int stageID;
	private int stageAttemptID;
	private TaskInfo taskInfo;

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

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public void setTaskInfo(TaskInfo taskInfo) {
		this.taskInfo = taskInfo;
	}

}
