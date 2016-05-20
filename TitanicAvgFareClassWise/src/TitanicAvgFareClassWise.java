import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TitanicAvgFareClassWise {

	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			int Count = 0;
			String Key = "Null";
			String Value = "Null";
			
			int IsClassFound = 0;
			int IsFareFound = 0; 
			
			// System.out.println(line);
			for (String retval : line.split(",")) {
				Count = Count + 1;
				//System.out.println(Count);
				if ((Count == 3)) {
					//System.out.println(retval);

					if (retval.matches("1")) {
						//System.out.println("Key. "+retval);
						Key = retval;
						IsClassFound = 1;
					}
					if (retval.matches("2")) {
						//System.out.println("Key.. "+retval);
						Key = retval;
						IsClassFound = 1;
					}
					if (retval.matches("3")) {
						//System.out.println("Key... "+retval);
						Key = retval;
						IsClassFound = 1;
					}
				}

				if ((Count == 10)) {
					//System.out.println("Value "+retval);
					IsFareFound = 1;
					Value = retval;
				}
				
			}
			

			if (IsClassFound == 1 && IsFareFound == 1) {
				//System.out.println(Key + " " + 1);
				Count = 0;
				IsClassFound = 0;
				IsFareFound = 0;
				context.write(new Text(Key), new DoubleWritable(Double.parseDouble(Value)));
				
			}
			

		}
	}

	// Reducer function after collecting the intermediate data count the values
	// according to each state
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int Avg = 0;
			for (DoubleWritable val : values) {
				Avg = Avg +1;
				sum = sum + val.get();
				
			}
			
			context.write(key, new DoubleWritable(sum/Avg));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();

		Job job = new Job();
		job.setJarByClass(TitanicAvgFareClassWise.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
			
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}