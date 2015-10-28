package bigdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Flight3 {

	public static void main(String[] args) throws Exception {

		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.printf("Usage: Flight3 <input dir> <output dir>\n");
			System.exit(-1);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job();

		job.setJarByClass(Flight3.class);

		job.setJobName("Flight-3");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Mapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(Reducer1.class);

		/*
		 * Start the first MapReduce job and wait for it to finish.
		 */

		boolean success = job.waitForCompletion(true);

		if (!success)
			System.exit(1);

		System.exit(0);

	}

	public static class Reducer1 extends
			Reducer<Text, IntWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			double avg = 0;
			double count = 0;

			Iterator<IntWritable> it = values.iterator();

			while (it.hasNext()) {
				double value = it.next().get();
				avg = (count * avg + value) / (count + 1.0);
				++count;
			}
			context.write(key, new DoubleWritable(avg));
		}
	}

	public static class Mapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (key.get() > 0) {
				String[] columns = value.toString().split(",");

				String season = null;

				if (Integer.parseInt(columns[1]) < 3
						|| Integer.parseInt(columns[1]) > 11) {
					// winter
					season = "Winter";
				} else if (Integer.parseInt(columns[1]) > 5
						&& Integer.parseInt(columns[1]) < 9) {
					// summer
					season = "Summer";
				} else
					return;
				
				//don't take into account NAs
				if("NA".equals(columns[14]))
					return;

				int delay = Integer.parseInt(columns[14]);

				context.write(new Text(season), new IntWritable(delay));
			}
		}
	}
}
