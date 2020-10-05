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

import java.text.SimpleDateFormat;  
import java.util.Date;  

public class Covid19_1 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private Text location = new Text();
		private LongWritable count = new LongWritable();
		private boolean world_count_included;
		private Date given_start_date, given_end_date, date;

        
        protected void setup(Mapper.Context context) throws IOException, InterruptedException
        {
            Configuration config = context.getConfiguration();
            world_count_included = Boolean.valueOf(config.get("world_count_included"));
        }
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {

			StringTokenizer tok = new StringTokenizer(value.toString(), "\n");

			try {

				given_start_date = new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01");
				given_end_date = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-08");

			} catch(ParseException e) {

				given_start_date = new Date(2020, 1, 1);
				given_end_date = new Date(2020, 4, 8);				
			
			}

			while (tok.hasMoreTokens()) {

				try {

					String[] values = tok.nextToken().split(",");

					boolean location_is_world = (values[1]).equals("World") || (values[1]).equals("International");

					if((location_is_world && world_count_included) || (!location_is_world)) {
						
						date = new SimpleDateFormat("yyyy-MM-dd").parse(values[0]);  

						location.set(values[1]);
						count.set(Long.parseLong(values[2]));

						if(date.compareTo(given_start_date)>=0 && date.compareTo(given_end_date)<=0)
							context.write(location, count);
					}

				} catch(NumberFormatException e) {
					System.out.println("Number Format Exception occured");
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
		conf.set("world_count_included", args[1]);
		Job myjob = Job.getInstance(conf, "Count of registered cases by location");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
