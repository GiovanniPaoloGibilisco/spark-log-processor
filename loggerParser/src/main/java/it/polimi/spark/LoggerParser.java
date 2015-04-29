package it.polimi.spark;

import it.polimi.spark.events.SparkEventsFactory;
import it.polimi.spark.events.SparkListenerEvent;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class LoggerParser {

	static final Logger logger = LoggerFactory.getLogger(LoggerParser.class);

	public static void main(String[] args) throws IOException {

		Config.init(args);
		Config config = Config.getInstance();

		if (!Files.exists(Paths.get(config.inputFile))) {
			logger.error("Input file does not exist");
			return;
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(config.inputFile));
			String line;
			List<SparkListenerEvent> loggedEvents = new ArrayList<SparkListenerEvent>();
			while ((line = reader.readLine()) != null) {
				JsonElement element = new JsonParser().parse(line);
				loggedEvents.add(SparkEventsFactory.buildEvent(element
						.getAsJsonObject()));
			}

			for (SparkListenerEvent event : loggedEvents)
				logger.debug(event.getClass().toString());

		} catch (JsonIOException e) {
			logger.error("Could not parse the input file", e);
		} catch (JsonSyntaxException e) {
			logger.error("Sintax error in the input file", e);
		} catch (FileNotFoundException e) {
			logger.error("Input file does not exist", e);
		} catch (IOException e) {
			logger.error("Error reading the file", e);
		} finally {
			if (reader != null)
				reader.close();
		}

	}

}
