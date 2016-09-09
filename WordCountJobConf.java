package au.rmit.bde;

/*
 * This code is an adapted version of the WordCount file we were provided in week 3
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJobConf extends Configured implements Tool {

	// Map Class
	static public class WordCountMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private LongWritable count = new LongWritable(1);
		// SET THE KEY VARIABLE
		private Text token = new Text();
		// SET THE VALUE VARIABLE
	//	private Text value = new Text(); 

		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {
			
			//FORMAT OF GOOGLEBOOKS N-GRAM DATASET
			//eg. for the 5-Gram phrase "lets get out of here"
			//
			// nGram					year	occurrences		pages	no.of books	
			//
			//lets get out of here		1998	235				235		235
			
			//read one line of the data and split based on tab delimiter 
			String[] nGram = text.toString().split("\\t+");
			//set the word or phrase to search for
			//project out (word, occurrences) so we can sum over all years
			token.set(nGram[0]);
			if(nGram.length >2){
				try{
					count.set(Long.parseLong(nGram[2]));
					context.write(token, count);
				}catch(NumberFormatException e){
					// ignore
				}
			}
			/*if (nGram[0].equalsIgnoreCase("fight club"))  
			{
				//set search term as key 
				token.set(nGram[0]);
				//set year and occurrences as the value
			 value.set(nGram[1]+"-"+nGram[2]);
				context.write(token, value);

			}*/
		}
	}

	// Reducer
	static public class WordCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();

		@Override
		public void reduce(Text token, Iterable<LongWritable> counts, Context context)
				throws IOException, InterruptedException {
			long n = 0;
			// Calculate sum of counts
			for (LongWritable count : counts) {
				n+=count.get();
			}
				total.set(n);
				context.write(token, total);
			
			
			
		}
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		// Initialising Map Reduce Job
		Job job = new Job(configuration, "Word Count");

		// Set Map Reduce main jobconf class
		job.setJarByClass(WordCountJobConf.class);

		// Set Mapper class
		job.setMapperClass(WordCountMapper.class);

		// Set Combiner class
		job.setCombinerClass(WordCountReducer.class);

		// set Reducer class
		job.setReducerClass(WordCountReducer.class);

		// set Input Format
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		// set Output key class
		job.setOutputKeyClass(Text.class);

		// set Output value class
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountJobConf(), args));
	}
}