import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.log4j.Logger;

public class InvertedIndex extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(InvertedIndex.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new InvertedIndex(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//    private long numRecords = 0;    
    private Text filename = new Text();
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
        
		// get the filename as a string
        String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		// create a text object of the filename.
        filename = new Text(filenameStr);
        
        Matcher nospecials = FILTER.matcher(word);
        if (nospecials.find()) {
        	currentWord = new Text(word);
            context.write(currentWord,filename);
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text word, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	StringBuilder stringBuilder = new StringBuilder();
    	String lastid = "";
		
		// loop through values which are filenames
		// store the last id in case  we have duplicates
    	for (Text value : values) {
    		if (!lastid.equals(value.toString().replace(".txt", ""))) {
    			lastid = value.toString().replace(".txt", "");
    			stringBuilder.append("("+ lastid +")");
    		}
    	}
    	
    	context.write(word, new Text(stringBuilder.toString()));
    }
  }
}
