import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class StopWordCount extends Configured implements Tool {
	
	private static final String STOPWORDS = "(the)|(of)|(and)";

	private static final Logger LOG = Logger.getLogger(StopWordCount.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new StopWordCount(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private String lastWord = "";
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		private static final Pattern FILTER = Pattern.compile("([A-Za-z]+)");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString().toLowerCase();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				} 

				// Filter out special signs
				Matcher nospecials = FILTER.matcher(word);
				if (nospecials.find()) {
					if (lastWord.matches(STOPWORDS)) {
						Text pairKey = new Text(lastWord + " " + word.toString());						
						context.write(pairKey, one);
					}
					lastWord = word;
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private int TOP = 5;
		
		private TreeMap<String, TreeMap<Integer, String>> stopWordRanks = new TreeMap<>(Collections.reverseOrder());
		
		@Override
		public void setup(Context context) throws IOException {

		}
		
		@Override
		public void reduce(Text wordPair, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			
			String stopwordKey = wordPair.toString().split(" ")[0];
			if (!stopWordRanks.containsKey(stopwordKey)) {
				TreeMap<Integer, String> top = new TreeMap<>(Collections.reverseOrder());
				stopWordRanks.put(stopwordKey, top);
			}
			
			stopWordRanks.get(stopwordKey).put(sum, wordPair.toString());
			if (stopWordRanks.get(stopwordKey).size() > TOP)
				stopWordRanks.get(stopwordKey).pollLastEntry();
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, TreeMap<Integer, String>> stopword : stopWordRanks.entrySet()) {
				TreeMap<Integer, String> ranglist = stopword.getValue();
				for (Entry<Integer, String> sumPairs : ranglist.entrySet()) {
					Text outText = new Text(sumPairs.getValue());
					context.write(outText, new IntWritable(sumPairs.getKey()));
				}
			}
		}
	}

	public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text words, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(words, new IntWritable(sum));
		}
	}
}
