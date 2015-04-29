package it.polimi.spark.events;

public class ExecutorInfo {
	private String host;
	private int totalCores;
	private String stdoutUrl;
	private String stderrUrl;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getTotalCores() {
		return totalCores;
	}

	public void setTotalCores(int totalCores) {
		this.totalCores = totalCores;
	}

	public String getStdoutUrl() {
		return stdoutUrl;
	}

	public void setStdoutUrl(String stdoutUrl) {
		this.stdoutUrl = stdoutUrl;
	}

	public String getStderrUrl() {
		return stderrUrl;
	}

	public void setStderrUrl(String stderrUrl) {
		this.stderrUrl = stderrUrl;
	}
}
