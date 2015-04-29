package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class TaskEndEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerTaskEnd";
	private int stageAttemptID;
	private int stageID;
	private String taskEndReason;
	private TaskInfo taskInfo;
	private TaskMetrics taskMetrics;

	private String taskType;

	public int getStageAttemptID() {
		return stageAttemptID;
	}

	public int getStageID() {
		return stageID;
	}

	public String getTaskEndReason() {
		return taskEndReason;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public TaskMetrics getTaskMetrics() {
		return taskMetrics;
	}

	public String getTaskType() {
		return taskType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TaskEndEvent initialize(JsonObject jsonObject) {
		stageID = jsonObject.get("Stage ID").getAsInt();
		stageAttemptID = jsonObject.get("Stage Attempt ID").getAsInt();
		taskType = jsonObject.get("Task Type").getAsString();
		taskEndReason = jsonObject.get("Task End Reason").getAsJsonObject()
				.get("Reason").getAsString();
		taskInfo = new TaskInfo().initialize(jsonObject.get("Task Info")
				.getAsJsonObject());
		taskMetrics = new TaskMetrics().initialize(jsonObject.get(
				"Task Metrics").getAsJsonObject());
		return this;
	}

	public void setStageAttemptID(int stageAttemptID) {
		this.stageAttemptID = stageAttemptID;
	}

	public void setStageID(int stageID) {
		this.stageID = stageID;
	}

	public void setTaskEndReason(String taskEndReason) {
		this.taskEndReason = taskEndReason;
	}

	public void setTaskInfo(TaskInfo taskInfo) {
		this.taskInfo = taskInfo;
	}

	public void setTaskMetrics(TaskMetrics taskMetrics) {
		this.taskMetrics = taskMetrics;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	
}
