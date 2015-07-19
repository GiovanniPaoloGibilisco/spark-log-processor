package it.polimi.spark.estimator;

import it.polimi.spark.dag.Stagenode;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.StreamCorruptedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Estimator {

	private static Config config;	
	static final Logger logger = LoggerFactory.getLogger(Estimator.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, SQLException {
	
		Config.init(args);
		config = Config.getInstance();

		if (config.usage) {
			config.usage();
			return;
		}
		
		if(config.benchmarkFolder != null){
			config.batch = true;
			
		}
		
	
		if(!config.isBatch()){
			AggregationEstimator estimator = new AggregationEstimator();
			estimator.estimateDuration();
		}else{
			ApplicationEstimator estimator = new ApplicationEstimator();
			estimator.estimateDuration();
		}
		



	}

	

}
