package it.polimi.spark.events;

import com.google.gson.JsonObject;

public class StageCompletedEvent extends SparkListenerEvent {

	static final String eventTag = "SparkListenerStageCompleted";

	private StageInfo stageInfo;

	public StageInfo getStageInfo() {
		return stageInfo;
	}

	@SuppressWarnings("unchecked")
	@Override
	public StageCompletedEvent initialize(JsonObject jsonObject) {	
		stageInfo = new StageInfo().initialize(jsonObject.get("Stage Info").getAsJsonObject());
		return this;
	}

	public void setStageInfo(StageInfo stageInfo) {
		this.stageInfo = stageInfo;
	}
	

}
