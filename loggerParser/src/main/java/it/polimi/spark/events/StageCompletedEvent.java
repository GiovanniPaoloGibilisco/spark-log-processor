package it.polimi.spark.events;

public class StageCompletedEvent {

	private StageInfo stageInfo;

	public StageInfo getStageInfo() {
		return stageInfo;
	}

	public void setStageInfo(StageInfo stageInfo) {
		this.stageInfo = stageInfo;
	}

}
