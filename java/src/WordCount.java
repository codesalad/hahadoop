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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

enum Records {
	UNIQUE_TERMS,
	DISTINCT_TERMS,
	COUNT_TS,
	TERMS_LT5,
	FILESTOTAL;
};

public class WordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WordCount(), args);
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
		private Text word = new Text();
		private Text filename = new Text();
		private long numRecords = 0;    
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		private static final Pattern FILTER = Pattern.compile("([A-Z][a-z]+)|([a-z]\\w+)");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString().toLowerCase();
			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				} 

				// Get filename the word is in.
				String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
				filename = new Text(filenameStr);
				context.write(filename, one);
				
				// Count number of words that start with T/t
				if (word.matches("^[Tt].*$"))
					context.getCounter(Records.COUNT_TS).increment(1);

				// Filter out special signs
				Matcher nospecials = FILTER.matcher(word);
				if (nospecials.find()) {
					currentWord = new Text(word);
					context.write(currentWord,one);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private int TOP = 5;
		private TreeMap<Integer, String> topWordsFrequencies = new TreeMap<>(Collections.reverseOrder());
		
		@Override
		public void setup(Context context) throws IOException {
			// Might be handy for later.
		}
		
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			if (!word.toString().matches("^.+txt$")) {
				int sum = 0;
				for (IntWritable count : counts) {
					sum += count.get();
				}
				
				//Count distinct elements
				if (sum == 1) 
					context.getCounter(Records.DISTINCT_TERMS).increment(1);
				
				// Count words with sum < 5
				if (sum < 5)
					context.getCounter(Records.TERMS_LT5).increment(1);
				
				topWordsFrequencies.put(sum, word.toString());
				if (topWordsFrequencies.size() > TOP)
					topWordsFrequencies.pollLastEntry();
					
				context.getCounter(Records.UNIQUE_TERMS).increment(1);
				context.write(word, new IntWritable(sum));	
			} else {
				// Count distinct files
				context.getCounter(Records.FILESTOTAL).increment(1);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			// Print words and frequencies from the topWordsFrequencies list.
			for (Entry<Integer, String> sumWords : topWordsFrequencies.entrySet()) {
				Text test = new Text(">>" + sumWords.getValue());
				context.write(test, new IntWritable(sumWords.getKey()));
			}
		}
	}

	public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
}
