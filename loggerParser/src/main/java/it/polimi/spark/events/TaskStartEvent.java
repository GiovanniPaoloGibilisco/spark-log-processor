package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class TaskStartEvent extends SparkListenerEvent {

	static final String eventTag = "SparkListenerTaskStart";
	private int stageAttemptID;
	private int stageID;

	private TaskInfo taskInfo;

	public int getStageAttemptID() {
		return stageAttemptID;
	}

	public int getStageID() {
		return stageID;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TaskStartEvent initialize(JsonObject jsonObject) {
		stageID = jsonObject.get("Stage ID").getAsInt();
		stageAttemptID = jsonObject.get("Stage Attempt ID").getAsInt();
		taskInfo = new TaskInfo().initialize(jsonObject.get("Task Info")
				.getAsJsonObject());
		return this;
	}

	public void setStageAttemptID(int stageAttemptID) {
		this.stageAttemptID = stageAttemptID;
	}

	public void setStageID(int stageID) {
		this.stageID = stageID;
	}

	public void setTaskInfo(TaskInfo taskInfo) {
		this.taskInfo = taskInfo;
	}
	

}
