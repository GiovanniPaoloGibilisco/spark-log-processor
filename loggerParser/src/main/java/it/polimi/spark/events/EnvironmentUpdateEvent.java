package it.polimi.spark.events;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class EnvironmentUpdateEvent extends SparkListenerEvent {
	static final String eventTag = "SparkListenerEnvironmentUpdate";
	private Map<String, String> classpathEntires;
	private Map<String, String> jvmInformation;
	private Map<String, String> sparkProperties;

	private Map<String, String> systemProperties;

	public Map<String, String> getClasspathEntires() {
		return classpathEntires;
	}

	public Map<String, String> getJvmInformation() {
		return jvmInformation;
	}

	public Map<String, String> getSparkProperties() {
		return sparkProperties;
	}

	public Map<String, String> getSystemProperties() {
		return systemProperties;
	}

	@SuppressWarnings("unchecked")
	@Override
	public EnvironmentUpdateEvent initialize(JsonObject jsonObject) {
		jvmInformation = new HashMap<String, String>();
		for (Entry<String, JsonElement> entry : jsonObject
				.get("JVM Information").getAsJsonObject().entrySet())
			jvmInformation.put(entry.getKey(), entry.getValue().getAsString());

		sparkProperties = new HashMap<String, String>();
		for (Entry<String, JsonElement> entry : jsonObject
				.get("Spark Properties").getAsJsonObject().entrySet())
			sparkProperties.put(entry.getKey(), entry.getValue().getAsString());

		systemProperties = new HashMap<String, String>();
		for (Entry<String, JsonElement> entry : jsonObject
				.get("System Properties").getAsJsonObject().entrySet())
			systemProperties
					.put(entry.getKey(), entry.getValue().getAsString());

		classpathEntires = new HashMap<String, String>();
		for (Entry<String, JsonElement> entry : jsonObject
				.get("Classpath Entries").getAsJsonObject().entrySet())
			classpathEntires
					.put(entry.getKey(), entry.getValue().getAsString());
		return this;
	}

	public void setClasspathEntires(Map<String, String> classpathEntires) {
		this.classpathEntires = classpathEntires;
	}

	public void setJvmInformation(Map<String, String> jvmInformation) {
		this.jvmInformation = jvmInformation;
	}

	public void setSparkProperties(Map<String, String> sparkProperties) {
		this.sparkProperties = sparkProperties;
	}
	
	public void setSystemProperties(Map<String, String> systemProperties) {
		this.systemProperties = systemProperties;
	}
	
}
