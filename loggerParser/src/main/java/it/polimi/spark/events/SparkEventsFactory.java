package it.polimi.spark.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

public class SparkEventsFactory {
	static final Logger logger = LoggerFactory
			.getLogger(SparkEventsFactory.class);

	public static SparkListenerEvent buildEvent(JsonObject jsonObject) {
		SparkListenerEvent event = null;
		switch (jsonObject.get("Event").getAsString()) {
		case ApplicationEndEvent.eventTag:
			return new ApplicationEndEvent().initialize(jsonObject);
		case ApplicationStartEvent.eventTag:
			return new ApplicationStartEvent().initialize(jsonObject);
		case BlockManagerAddedEvent.eventTag:
			return new BlockManagerAddedEvent().initialize(jsonObject);
		case EnvironmentUpdateEvent.eventTag:
			return new EnvironmentUpdateEvent().initialize(jsonObject);
		case ExecutorAddedEvent.eventTag:
			return new ExecutorAddedEvent().initialize(jsonObject);
		case JobEndEvent.eventTag:
			return new JobEndEvent().initialize(jsonObject);
		case JobStartEvent.eventTag:
			return new JobStartEvent().initialize(jsonObject);
		case LogStartEvent.eventTag:
			return new LogStartEvent().initialize(jsonObject);
		case StageCompletedEvent.eventTag:
			return new StageCompletedEvent().initialize(jsonObject);
		case StageSubmittedEvent.eventTag:
			return new StageSubmittedEvent().initialize(jsonObject);
		case TaskEndEvent.eventTag:
			return new TaskEndEvent().initialize(jsonObject);
		case TaskStartEvent.eventTag:
			return new TaskStartEvent().initialize(jsonObject);
		default:
			logger.warn("No event found for this json object", jsonObject);
			break;
		}
		return event;
	}

}
