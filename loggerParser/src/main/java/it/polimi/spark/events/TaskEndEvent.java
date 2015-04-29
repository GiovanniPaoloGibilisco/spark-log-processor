package it.polimi.spark.events;

public class TaskEndEvent {
private int stageID;
private int stageAttemptID;
private String taskType;
private String taskEndReason;
private TaskInfo taskInfo;
	private TaskMetrics taskMetrics;

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

	public String getTaskType() {
		return taskType;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public String getTaskEndReason() {
		return taskEndReason;
	}

	public void setTaskEndReason(String taskEndReason) {
		this.taskEndReason = taskEndReason;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public void setTaskInfo(TaskInfo taskInfo) {
		this.taskInfo = taskInfo;
	}

	public TaskMetrics getTaskMetrics() {
		return taskMetrics;
	}

	public void setTaskMetrics(TaskMetrics taskMetrics) {
		this.taskMetrics = taskMetrics;
	}
}
