package it.polimi.spark.events;

public class StageSubmittedEvent extends SparkListenerEvent {

	private StageInfo stageInfo;

	public StageInfo getStageInfo() {
		return stageInfo;
	}

	public void setStageInfo(StageInfo stageInfo) {
		this.stageInfo = stageInfo;
	}

}
