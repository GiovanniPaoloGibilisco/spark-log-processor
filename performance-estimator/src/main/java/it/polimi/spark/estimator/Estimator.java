package it.polimi.spark.estimator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Estimator {

	private static Config config;	
	public static List<EstimationResult> results;
	public static String estimationFunction;
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
			results = estimator.estimateDuration();
			estimationFunction = estimator.getEstimationFunction();
		}		
		



	}

	

}
