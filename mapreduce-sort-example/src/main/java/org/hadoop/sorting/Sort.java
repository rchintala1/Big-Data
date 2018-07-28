package org.hadoop.sorting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by nsonmez
 */

/**
 * this mapreduce job sorts the given values from a txt file given a data file
 * like this: ...
 */
public class Sort {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Mean <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf);
		job.setJobName("Sort");
		job.setJarByClass(Sort.class);
		/*
		 * Even though I wrote the mapper and reducer, I'm using Mapper.class &
		 * Reducer.class because in the suffle and sort part where the mappings
		 * from the mapper goes to the reducers, things are already sorted.
		 * job.setMapperClass(SortMapper.class);
		 * job.setReducerClass(SortReducer.class);
		 */
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class SortMapper extends Mapper<Object, Text, Text, Text> {
		private Map<Text, List<Text>> sortedMap = new HashMap<>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String values = value.toString();
			String character = values.substring(0, 1);
			Text firstCharacter = new Text(character);

			if (!sortedMap.containsKey(firstCharacter)) {

				List<Text> newCharacterTreeSet = new ArrayList<>();
				newCharacterTreeSet.add(value);
				sortedMap.put(firstCharacter, newCharacterTreeSet);
			}
			sortedMap.get(firstCharacter).add(value);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text firstCharacter : sortedMap.keySet()) {
				List<Text> sortedIterator = sortedMap.get(firstCharacter);
				Collections.sort(sortedIterator);
				for (Text sortedText : sortedIterator) {
					context.write(firstCharacter, sortedText);
				}
			}
		}
	}

	public static class SortReducer extends Reducer<Text, Text, Text, Text> {
		MultipleOutputs mos = null;
		private Map<Text, List<Text>> sortedMapReducer = new HashMap<>();

		public void reduce(Text key, Text sortedValues, Context context) throws IOException, InterruptedException {
			if (sortedMapReducer.containsKey(key)) {
				List<Text> existingKey = sortedMapReducer.get(key);
				existingKey.add(sortedValues);
				sortedMapReducer.put(key, existingKey);
			} else {
				List<Text> addingNewKey = new ArrayList<Text>();
				addingNewKey.add(sortedValues);
				sortedMapReducer.put(key, addingNewKey);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text lastIteration : sortedMapReducer.keySet()) {
				List<Text> sortedvalues = sortedMapReducer.get(lastIteration);
				Collections.sort(sortedvalues);
				for (Text sortedText : sortedvalues) {
					context.write(sortedText, lastIteration);
				}
			}
		}
	}
}