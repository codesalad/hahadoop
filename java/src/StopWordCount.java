import java.io.IOException;
import java.util.Collections;
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
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Pair<String, String>, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private String lastWord = "";
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		private static final Pattern FILTER = Pattern.compile("([A-Z][a-z]+)|([a-z]\\w+)");

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
					
					if (lastWord.matches("(the)|(of)|(and)")) {
						Pair<String, String> pair = new Pair<String, String>(lastWord, word);
						context.write(pair, one);
					}
					
					lastWord = word;
				}
			}
		}
	}

	public static class Reduce extends Reducer<Pair<String, String>, IntWritable, Text, IntWritable> {
		private int TOP = 5;
		private TreeMap<Integer, Pair<String, String>> topWordsFrequencies = new TreeMap<>(Collections.reverseOrder());
		
		@Override
		public void setup(Context context) throws IOException {
			// Might be handy for later.
		}
		
		@Override
		public void reduce(Pair<String, String> words, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}

			topWordsFrequencies.put(sum, words);
			if (topWordsFrequencies.size() > TOP)
				topWordsFrequencies.pollLastEntry();
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			// Print words and frequencies from the topWordsFrequencies list.
			for (Entry<Integer, Pair<String, String>> sumPairs : topWordsFrequencies.entrySet()) {
				Text test = new Text(sumPairs.getValue().getFirst() + " " + sumPairs.getValue().getSecond());
				context.write(test, new IntWritable(sumPairs.getKey()));
			}
		}
	}

	public static class Combine extends Reducer<Pair<String, String>, IntWritable, Pair<String, String>, IntWritable> {
		@Override
		public void reduce(Pair<String, String> words, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(words, new IntWritable(sum));
		}
	}
}
