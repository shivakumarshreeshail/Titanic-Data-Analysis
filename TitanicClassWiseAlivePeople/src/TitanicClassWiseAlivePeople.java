import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TitanicClassWiseAlivePeople {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int Count = 0;
			
			int IsSurvived = 0, IsEmbarked_S = 0;
			String Key = "Null";
			
			String line = value.toString();
			//System.out.println(line);
			for (String retval : line.split(",")) {
				Count = Count + 1;
				// System.out.println(Count);
				if ((Count == 2)) {
					// System.out.println(retval);`

					if (retval.matches("0")) {
						// System.out.println("Key. "+retval);
						IsSurvived = 1;

					}

				}

				if ((Count == 3)) {
					// System.out.println(retval);

					if (retval.matches("1")) {
						// System.out.println("Key. "+retval);
						Key = retval;
					}
					if (retval.matches("2")) {
						// System.out.println("Key. "+retval);
						Key = retval;
					}
					if (retval.matches("3")) {
						// System.out.println("Key. "+retval);
						Key = retval;
					}

				}

				if ((Count == 12)) {
					if (retval.matches("S")) {
						// System.out.println("Key. "+retval);
						IsEmbarked_S = 1;
					}
				}
			}

			if (IsEmbarked_S == 1 && IsSurvived == 1) {
				//System.out.println(Key + " " + 1);
				Count = 0;
				IsEmbarked_S = 0;
				IsSurvived = 0;
				context.write(new Text(Key), new IntWritable(1));
			}
			

		}
	}

	// Reducer function after collecting the intermediate data count the values
	// according to each state
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum = sum + val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();

		Job job = new Job();
		job.setJarByClass(TitanicClassWiseAlivePeople.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}