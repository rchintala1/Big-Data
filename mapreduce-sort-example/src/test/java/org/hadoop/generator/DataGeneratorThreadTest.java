package org.hadoop.generator;

import java.io.InputStream;
import java.util.Properties;

import org.hadoop.generation.DataGeneratorThread;
import org.junit.Test;

public class DataGeneratorThreadTest {
	@Test(expected = RuntimeException.class)
	public void randomStringTestLengthZero() {
		DataGeneratorThread myThr = new DataGeneratorThread();
		int value = 0;
		myThr.randomString(0, value);
	}

	@Test(expected = RuntimeException.class)
	public void randomStringTestLengthGreatorThanTwelve() {
		DataGeneratorThread myThr = new DataGeneratorThread();
		int value = 0;
		myThr.randomString(15, value);
	}

	@Test(expected = RuntimeException.class)
	public void randomStringTestValueNotBetweenZeroAndSix() {
		DataGeneratorThread myThr = new DataGeneratorThread();
		myThr.randomString(12, 8);
	}

	@Test
	public void randomStringTestCorrectValues() {
		DataGeneratorThread myThr = new DataGeneratorThread();
		myThr.randomString(12, 6);
	}

	@Test
	public void runValidInput() throws Exception {
		Properties prop = new Properties();
		InputStream input = null;
		input = DataGeneratorThreadTest.class.getClassLoader().getResourceAsStream("config.properties");

		// load a properties file
		prop.load(input);
		String outputFileDirectory = (String) prop.get("outputFileDirectoryValid");
		String outputFileName = (String) prop.get("outputFileNameValid");
		DataGeneratorThread myThr = new DataGeneratorThread(outputFileDirectory, outputFileName);
		myThr.run();
	}

	@Test(expected = RuntimeException.class)
	public void runNullInput() throws Exception {
		DataGeneratorThread myThr = new DataGeneratorThread(null, null);
		myThr.run();
	}

	@Test(expected = RuntimeException.class)
	public void runInvalidDirectoryName() throws Exception {
		Properties prop = new Properties();
		InputStream input = null;
		input = DataGeneratorThreadTest.class.getClassLoader().getResourceAsStream("config.properties");

		// load a properties file
		prop.load(input);
		String outputFileDirectory = (String) prop.get("outputFileDirectoryInvalid");
		String outputFileName = (String) prop.get("outputFileNameValid");
		DataGeneratorThread myThr = new DataGeneratorThread(outputFileDirectory, outputFileName);
		myThr.run();
	}
}
