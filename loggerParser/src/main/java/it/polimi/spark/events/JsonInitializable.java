package it.polimi.spark.events;

import com.google.gson.JsonObject;

public interface JsonInitializable {
	<T> T initialize(JsonObject jsonObject);

}
