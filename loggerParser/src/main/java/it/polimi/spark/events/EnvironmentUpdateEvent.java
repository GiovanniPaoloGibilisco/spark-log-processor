package it.polimi.spark.events;

import java.util.Map;

public class EnvironmentUpdateEvent {
	private Map<String, String> jvmInformation;
	private Map<String, String> sparkProperties;
	private Map<String, String> systemProperties;
	private Map<String, String> classpathEntires;

	public Map<String, String> getJvmInformation() {
		return jvmInformation;
	}

	public void setJvmInformation(Map<String, String> jvmInformation) {
		this.jvmInformation = jvmInformation;
	}

	public Map<String, String> getSparkProperties() {
		return sparkProperties;
	}

	public void setSparkProperties(Map<String, String> sparkProperties) {
		this.sparkProperties = sparkProperties;
	}

	public Map<String, String> getSystemProperties() {
		return systemProperties;
	}

	public void setSystemProperties(Map<String, String> systemProperties) {
		this.systemProperties = systemProperties;
	}

	public Map<String, String> getClasspathEntires() {
		return classpathEntires;
	}

	public void setClasspathEntires(Map<String, String> classpathEntires) {
		this.classpathEntires = classpathEntires;
	}
}
