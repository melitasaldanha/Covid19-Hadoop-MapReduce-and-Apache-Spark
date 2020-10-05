import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.text.ParseException; 

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_2 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {

		private Text location = new Text();
		private LongWritable count = new LongWritable();
		private Date start_date, end_date, date;

        
        protected void setup(Mapper.Context context) throws IOException, InterruptedException
        {
            Configuration config = context.getConfiguration();

            try {

	            start_date = new SimpleDateFormat("yyyy-MM-dd").parse(config.get("start_date"));
	            end_date = new SimpleDateFormat("yyyy-MM-dd").parse(config.get("end_date"));

	        }  catch(ParseException e) {

				System.out.println("Parse Exception in input date range");
				System.exit(1);
			}
        }
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {

			StringTokenizer tok = new StringTokenizer(value.toString(), "\n");
            
			while (tok.hasMoreTokens()) {

				try {

					String[] values = tok.nextToken().split(",");

					date = new SimpleDateFormat("yyyy-MM-dd").parse(values[0]);  

					location.set(values[1]);
					count.set(Long.parseLong(values[3]));

					if((date.compareTo(start_date) >= 0) && (date.compareTo(end_date) <= 0))
						context.write(location, count);

				} catch(NumberFormatException e) {
					System.out.println("Number Format Exception Occured");
				} catch(ArrayIndexOutOfBoundsException e) {
					System.out.println("Array out of Bounds Exception occured");
				} catch(ParseException e) {
					System.out.println("Parse Exception occured");
				}
			}
		}
		
	}
	
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			long sum = 0;

			for (LongWritable tmp: values)
				sum += tmp.get();
					
			total.set(sum);

			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		conf.set("start_date", args[1]);
		conf.set("end_date", args[2]);
		Job myjob = Job.getInstance(conf, "Count of deaths by location in a given date range");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
