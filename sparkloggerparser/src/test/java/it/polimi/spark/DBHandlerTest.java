package it.polimi.spark;

import java.sql.SQLException;

import org.junit.Test;

public class DBHandlerTest {

	@Test
	public void testDBHandler() {
		DBHandler handler = new DBHandler("com.mysql.jdbc.Driver",
				"jdbc:mysql://minli39.sl.cloud9.ibm.com/SparkBench", "test",
				"testpwd");
		handler.close();
	}

	@Test
	public void testInsertBenchmark() throws SQLException {
		DBHandler handler = new DBHandler("com.mysql.jdbc.Driver",
				"jdbc:mysql://minli39.sl.cloud9.ibm.com/SparkBench", "test",
				"testpwd");
		Benchmark testApplication = new Benchmark("minli39", "testAppID");
		testApplication.setAppName("testApp");
		testApplication.setDataSize(15.2);
		testApplication.setDuration(12345678);
		testApplication.setParallelism(800);
		testApplication.setDriverMemory(3);
		testApplication.setShuffleMemoryFraction(0.3);
		
		
		Job testJob1 = new Job(testApplication.getClusterName(), testApplication.getAppID(), 1);
		testJob1.setDuration(1542);
		Job testJob2 = new Job(testApplication.getClusterName(), testApplication.getAppID(), 2);
		testJob2.setDuration(548);
		testApplication.addJob(testJob1);
		testApplication.addJob(testJob2);
		
		
		Stage testStage = new Stage(testJob1.getClusterName(), testJob1.getAppID(), testJob1.getJobID(), 1);
		testStage.setInputSize(10.5);
		testJob1.addStage(testStage);
			
		
		handler.insertBenchmark(testApplication);
	}

}
