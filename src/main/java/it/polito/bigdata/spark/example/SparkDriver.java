package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		String inputPath = args[0];
		String outputPath = args[1];
		String prefix = args[2].toLowerCase();

	
		// Create a configuration object and set the name of the application
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		SparkConf conf = new SparkConf()
							.setAppName("Spark Lab #5 - filtering amazon data and compute some statistics")
							.setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFrequenciesRDD = sc.textFile(inputPath);

		/*
		 * Task 1:
		 * keep only the lines with a word starting by the prefix specified by the user
		*/
		
		JavaRDD<String> filteredWordFrequencies = 
						wordFrequenciesRDD.filter(line -> line.trim().toLowerCase().startsWith(prefix));
		
		long numSelectedLines = filteredWordFrequencies.count();
		
		JavaPairRDD<String, Integer> wordFrequenciesPairs = 
				filteredWordFrequencies.mapToPair(line -> {
					String[] fields = line.split("\t");
					String word = fields[0];
					int frequency = Integer.parseInt(fields[1]);
					return new Tuple2<String,Integer>(word,frequency);
				});	
		
		JavaRDD<Integer> onlyFrequencies = wordFrequenciesPairs.values();
		int maxFrequency = onlyFrequencies.top(1).get(0);
		
		// print the two required statistics on the standard output of the driver
		System.out.println(String.format("%-28s  %d\n%-28s  %d\n", 
							"- Number of selected lines:", numSelectedLines,
							"- Maximum frequency:", maxFrequency));
		
		/*
		 * Task 2: 
		 * keep only the lines with a word having a frequency greater than 80% of the maximum frequency
		 */
		
		JavaPairRDD<String, Integer> wordFrequenciesGreaterThan80 = 
				wordFrequenciesPairs.filter(tuple -> 
						(double)tuple._2 > 0.8 * (double)maxFrequency);
		
		// print the required statistic
		long numMostFrequentWords = wordFrequenciesGreaterThan80.count();
		
		System.out.println("\n- Number of most frequent words (freq > 80%): " + numMostFrequentWords);
		
		JavaRDD<String> mostFrequentWords = wordFrequenciesGreaterThan80.keys();
		mostFrequentWords.saveAsTextFile(outputPath);
				
		// Close the Spark context
		sc.close();
	}
}
