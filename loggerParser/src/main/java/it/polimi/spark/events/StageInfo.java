package it.polimi.spark.events;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class StageInfo implements JsonInitializable {
	private String details;
	private int numberOfTasks;
	private List<RDDInfo> rddInfo;
	private int stageAttemptID;
	private int stageID;
	private String stageName;
	private Long sumbissionTime;

	public String getDetails() {
		return details;
	}

	public int getNumberOfTasks() {
		return numberOfTasks;
	}

	public List<RDDInfo> getRddInfo() {
		return rddInfo;
	}

	public int getStageAttemptID() {
		return stageAttemptID;
	}

	//TODO: find out what the Accumulable tag is used for in the log file. 
	public int getStageID() {
		return stageID;
	}

	public String getStageName() {
		return stageName;
	}

	public Long getSumbissionTime() {
		return sumbissionTime;
	}

	@SuppressWarnings("unchecked")
	@Override
	public StageInfo initialize(JsonObject jsonObject) {
		if (jsonObject == null)
			return null;
		stageID = jsonObject.get("Stage ID").getAsInt();
		stageAttemptID = jsonObject.get("Stage Attempt ID").getAsInt();
		stageName = jsonObject.get("Stage Name").getAsString();
		numberOfTasks = jsonObject.get("Number of Tasks").getAsInt();
		rddInfo = new ArrayList<RDDInfo>();
		JsonArray infos = jsonObject.get("RDD Info").getAsJsonArray();
		for(int i=0;i<infos.size();i++)
			rddInfo.add(new RDDInfo().initialize(infos.get(i).getAsJsonObject()));		
		details = jsonObject.get("Details").getAsString(); 
		return this;
	}

	public void setDetails(String details) {
		this.details = details;
	}

	public void setNumberOfTasks(int numberOfTasks) {
		this.numberOfTasks = numberOfTasks;
	}

	public void setRddInfo(List<RDDInfo> rddInfo) {
		this.rddInfo = rddInfo;
	}

	public void setStageAttemptID(int stageAttemptID) {
		this.stageAttemptID = stageAttemptID;
	}

	public void setStageID(int stageID) {
		this.stageID = stageID;
	}

	public void setStageName(String stageName) {
		this.stageName = stageName;
	}

	public void setSumbissionTime(Long sumbissionTime) {
		this.sumbissionTime = sumbissionTime;
	}

}
