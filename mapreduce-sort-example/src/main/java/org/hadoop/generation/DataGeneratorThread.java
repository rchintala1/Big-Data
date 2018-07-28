package org.hadoop.generation;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

public class DataGeneratorThread implements Runnable {
	private String charsCaps = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private String chars = "abcdefghijklmnopqrstuvwxyz";
	private String nums = "0123456789";
	private String alphanum = charsCaps + chars + nums;
	private String alpha = charsCaps + chars;
	private Random rnd = new Random();
	private String outputPathDirectory;
	private String outputFileName;

	public DataGeneratorThread(String outputPathDirectory, String outputFileName) {
		this.outputPathDirectory = outputPathDirectory;
		this.outputFileName = outputFileName;
	}

	public DataGeneratorThread() {
	}

	public void run() {
		try {
			File file = new File(this.outputPathDirectory + "/" + this.outputFileName + "-"
					+ Thread.currentThread().getId() + ".txt");
			file.createNewFile();
			FileWriter writer = new FileWriter(file);
			Random rand = new Random();
			for (long length = 0; length <= 85000000; length ++) {
				int value = rand.nextInt(6);
				writer.write(randomString(12, value));
				writer.write("\n");
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			throw new RuntimeException("Error while processing the file");
		}
	}

	public char[] randomString(int len, int value) {
		char[] password = new char[len];
		int alphanumL = alphanum.length();
		int numL = nums.length();
		int alphabetL = alpha.length();
		if (len != 12) {
			throw new RuntimeException("The length of the word can not be greater than 12");
		} else if (value < 0 || value > 6) {
			throw new RuntimeException("The value should be in between 0 and 6");
		} else {
			for (int i = 0; i < len; i++) {
				if (value == 0) {
					password[i] = alphanum.charAt(rnd.nextInt(alphanumL));
				} else if (value == 1) {
					password[i] = nums.charAt(rnd.nextInt(numL));
				} else {
					password[i] = alpha.charAt(rnd.nextInt(alphabetL));
				}
			}
		}
		return password;
	}
}