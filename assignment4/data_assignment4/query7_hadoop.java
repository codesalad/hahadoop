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

/**
* wnguyen, 4287118
* Outputs players with a salary above 500,000 in 2001 who have more than 50 homeruns. 
* Output the players, their amount of homeruns and their salary.
* Arguments: <hdfs folder containingg .csv files> <outfolder>
*/
public class Salaries_batting extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Salaries_batting.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Salaries_batting(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			String line = lineText.toString();			
			String[] parts = line.split(",");
						
			// Skips first line (header of csv file)
			if (!line.matches("playerID,yearID,stint,teamID,lgID,G,G_batting,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,G_old") 
					&& !line.matches("yearID,teamID,lgID,playerID,salary")) {

				// If line is from batting csv
				if (parts.length >= 12) {
					String playerID = parts[0];
					int yearID = parts[1].isEmpty()? 0 : Integer.parseInt(parts[1]);
					int HR = parts[12].isEmpty() ? 0 : Integer.parseInt(parts[12]);
					
					// Filter by year 2001 and HR > 50
					if (yearID == 2001 && HR > 50)
						context.write(new Text(playerID), new Text("HR:"+HR));

				// If line is from salaries csv
				} else if (parts.length == 5) {
					String playerID = parts[3];
					int yearID = parts[0].isEmpty()? 0 : Integer.parseInt(parts[0]);
					int salary = parts[4].isEmpty()? 0 : Integer.parseInt(parts[4]);
					
					// Filter by year 2001 and salary > 500,000
					if (yearID == 2001 && salary > 500000)
						context.write(new Text(playerID), new Text("salary:"+salary));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void setup(Context context) throws IOException {
			// Might be handy for later.
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int HR = -1;
			int salary = -1;
			
			// Parse values
			for (Text value : values) {
				String[] split = value.toString().split(":");
				if (split[0].equals("HR")) {
					HR = Integer.parseInt(split[1]);
				} else if (split[0].equals("salary")) {
					salary = Integer.parseInt(split[1]);
				}
			}
			
			// If both properties are found, write to file
			if (HR != -1 && salary != -1)
				context.write(key, new Text(HR + "\t" + salary));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
		}
	}
}
