package org.hadoop.generator;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.hadoop.generation.DataGenerator;
import org.junit.Test;

public class DataGeneratorTest {

	@Test(expected = RuntimeException.class)
	public void mergeFilesInvalidInput() {
		DataGenerator dataGen = new DataGenerator();
		dataGen.mergeFiles(null, null);
	}

	@Test(expected = RuntimeException.class)
	public void mergeFilesFilesArrayEmpty() throws Exception {
		DataGenerator dataGen = new DataGenerator();

		Properties prop = new Properties();
		InputStream input = null;
		input = DataGeneratorTest.class.getClassLoader().getResourceAsStream("config.properties");

		// load a properties file
		prop.load(input);
		String outputPathDirectory = (String) prop.get("outputFileDirectoryValid");
		String outputFileName = (String) prop.get("outputFileNameValid");
		
		String finalMergedFile = outputPathDirectory + "/" + outputFileName + ".txt";
		
		File mergedFile = new File(finalMergedFile);
		
		File[] listOfFiles = new File[0];
		
		dataGen.mergeFiles(listOfFiles, mergedFile);
	}
	
	@Test(expected = RuntimeException.class)
	public void mergeFilesEmpty() throws Exception {
		DataGenerator dataGen = new DataGenerator();

		Properties prop = new Properties();
		InputStream input = null;
		input = DataGeneratorTest.class.getClassLoader().getResourceAsStream("config.properties");

		// load a properties file
		prop.load(input);
		String outputPathDirectory = (String) prop.get("outputFileDirectoryInvalid");
		String outputFileName = (String) prop.get("outputFileNameValid");
		
		String finalMergedFile = outputPathDirectory + "/" + outputFileName + ".txt";
		
		File mergedFile = new File(finalMergedFile);
		File folder = new File(outputPathDirectory);
		File[] listOfFiles = folder.listFiles();
		
		dataGen.mergeFiles(listOfFiles, mergedFile);
	}
}