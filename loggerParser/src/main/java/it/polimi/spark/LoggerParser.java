package it.polimi.spark;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
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
			Map<String, JsonObject> eventMaps = new HashMap<String, JsonObject>();
			while ((line = reader.readLine()) != null) {
				JsonElement element = new JsonParser().parse(line);
				String eventName = element.getAsJsonObject().get("Event")
						.getAsString();
				eventMaps.put(eventName, element.getAsJsonObject());
			}
			for (String s : eventMaps.keySet())
				logger.info(s);
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
