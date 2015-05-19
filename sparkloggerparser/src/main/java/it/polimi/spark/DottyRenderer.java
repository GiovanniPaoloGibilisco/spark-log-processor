package it.polimi.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Utility to launch dotty in order to render a dag into an image
 * 
 * @author Giovanni paolo gibilisco
 *
 */
public class DottyRenderer extends Thread {

	private String inputFile;
	private String imageFilename;
	private String imageExtension;
	Config config;

	static final Logger logger = LoggerFactory.getLogger(DottyRenderer.class);

	/**
	 * @param inputFile
	 * @param imageFilename
	 * @param imageExtension
	 */
	public DottyRenderer(String inputFile, String imageFilename,
			String imageExtension) {
		super();
		this.inputFile = inputFile;
		this.imageFilename = imageFilename;
		this.imageExtension = imageExtension;
		config = Config.getInstance();
	}

	@Override
	public void run() {
		logger.info("Rendering application DAG image");
		Process dotProcess;
		String inputFilePath = Paths.get(config.outputFolder, inputFile)
				.toAbsolutePath().toString();
		String outputFilePath = Paths
				.get(config.outputFolder, imageFilename +"."+ imageExtension)
				.toAbsolutePath().toString();
		try {
			logger.info("Running: dot -T " + imageExtension + inputFilePath
					+ " -o " + outputFilePath);
			dotProcess = new ProcessBuilder("dot", "-T" + imageExtension,
					inputFilePath, "-o " + outputFilePath).start();

			InputStream is = dotProcess.getErrorStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null) {
				logger.info("Rendering " + imageFilename + imageExtension
						+ ": " + line);
			}
			br.close();
			logger.info(imageFilename + imageExtension + " has been rendered");
		} catch (IOException e) {
			logger.error("Error while rendering image", e);
		}
	}

	/**
	 * check if the executable dot is available
	 * 
	 * @throws IOException
	 */
	public static boolean isDottyAvailable() throws IOException {
		Process dotProcess = new ProcessBuilder("dot", "-V").start();
		InputStream is = dotProcess.getErrorStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line = null;
		boolean available = false;
		while ((line = br.readLine()) != null) {
			logger.info("Output of dot is:" + line);
			if (line.contains("dot - graphviz version"))
				available = true;
		}
		br.close();
		return available;
	}
}
