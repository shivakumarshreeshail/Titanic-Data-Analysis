import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TitanicNoMaleFemaleDiedInEachClass {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int Count = 0;
			@SuppressWarnings("unused")
			int IsSurvived = 0, IsEmbarked_S = 0;
			String Key = "Null";
			
			int MaleCount = 0 , FemaleCount = 0;
			
			String line = value.toString();
			
			// System.out.println(line);
			for (String retval : line.split(",")) {
				Count = Count + 1;
				// System.out.println(Count);
				if ((Count == 2)) {
					// System.out.println(retval);`

					if (retval.matches("1")) {
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
				if ((Count == 5)) {

					if (retval.matches("^male$")) {
						MaleCount = 1;
					}
					if (retval.matches("^female$")) {
						FemaleCount = 1;
					}
				}
			}

			if (MaleCount == 1 && IsSurvived == 1) {
				Key = "Male   "+Key;
				//System.out.println("Key" + " " + Key+" Value 1");
				MaleCount = 0;
				IsSurvived = 0;
				context.write(new Text(Key), new IntWritable(1));
			}else if(FemaleCount == 1 && IsSurvived == 1){
				Key = "Female "+Key;
				//System.out.println("Key" + " " + Key+" Value 1");
				FemaleCount = 0;
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
			String Test="Null";
			
			Test = key.toString();
			//if (Test.matches("Female1") ){
				context.write(new Text(Test), new IntWritable(sum));
			//}
			
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();

		Job job = new Job();
		job.setJarByClass(TitanicNoMaleFemaleDiedInEachClass.class);
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