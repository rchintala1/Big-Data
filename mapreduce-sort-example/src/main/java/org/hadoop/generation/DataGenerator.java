package org.hadoop.generation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataGenerator {
	public static void main(String[] args) throws Exception {
		try {
			generateData(args);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public static void generateData(String[] args) throws IOException, InterruptedException {
		if (args.length != 1) {
			System.out.println("Please specify file size");
			System.exit(0);
		}
		
		int outputFileSize = Integer.parseInt(args[0]);
		
		// load a properties file
		String outputPathDirectory = "output";
		String outputFileName = "outputFile";
		String finalMergedFile = System.getProperty("user.dir") + "/" + outputPathDirectory + "/" + outputFileName
				+ ".txt";
		new File(outputPathDirectory).mkdir();
		
		// start 10 executor threads which will create multiple files of 1GB.
		ExecutorService executor = Executors.newCachedThreadPool();
		for (int i = 0; i < outputFileSize; i++) {
			executor.execute(new DataGeneratorThread(outputPathDirectory, outputFileName));
		}
		executor.shutdown();
		executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);

		// check if merged file already exists in the path. if yes delete it.
		File mergedFile = new File(finalMergedFile);
		if (mergedFile.exists()) {
			mergedFile.delete();
		}

		// Get list of all files of 1GB
		File folder = new File(outputPathDirectory);
		File[] listOfFiles = folder.listFiles();

		// create a new merged file
		mergedFile.createNewFile();

		// merge individual files
		DataGenerator dataGen = new DataGenerator();
		if (mergedFile.exists() && listOfFiles.length != 0) {
			dataGen.mergeFiles(listOfFiles, mergedFile);
		} else {
			throw new RuntimeException("Merged file doesnt exist or files array is empty");
		}
	}

	public void mergeFiles(File[] files, File mergedFile) {
		if (!mergedFile.exists() || files.length == 0) {
			throw new RuntimeException("Merged file doesnt exist or files array is empty");
		}
		FileWriter fstream = null;
		BufferedWriter out = null;
		try {
			fstream = new FileWriter(mergedFile, true);
			out = new BufferedWriter(fstream);
			for (File f : files) {
				if (!f.getName().contains(".txt")) {
					continue;
				}
				FileInputStream fis;
				fis = new FileInputStream(f);
				BufferedReader in = new BufferedReader(new InputStreamReader(fis));

				String aLine;
				while ((aLine = in.readLine()) != null) {
					aLine = aLine.trim(); // remove leading and trailing
											// whitespace
					if (!aLine.equals("")) // don't write out blank lines
					{
						out.write(aLine);
						out.newLine();
					}
				}
				in.close();
			}
			out.close();

			// delete all the individual files.
			for (File f : files) {
				f.delete();
			}
		} catch (IOException e) {
			throw new RuntimeException("Merged file doesnt exist or files array is empty");
		}
	}
}