import java.io.IOException;
import java.util.*; 
import java.io.*;
import java.net.URI;
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
import org.apache.hadoop.fs.FileSystem;

public class Covid19_3 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, FloatWritable> {

		private Text location = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {

			StringTokenizer tok = new StringTokenizer(value.toString(), "\n");
            
			while (tok.hasMoreTokens()) {

				try {

					String[] values = tok.nextToken().split(",");

					location.set(values[1]);
					FloatWritable count = new FloatWritable(Float.parseFloat(values[2]));

					context.write(location, count);

				} catch(NumberFormatException e) {
					System.out.println("Number Format Exception occured");
				} catch(ArrayIndexOutOfBoundsException e) {
					System.out.println("Array out of Bounds Exception occured");
				}
			}
		}
		
	}
	
	
	public static class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable pop_per_1_mil = new FloatWritable();
		private HashMap<Text, Long> country_pop = new HashMap<Text, Long>();
		private long one_million = 1000000;

		public void setup(Context context) {

			try {				

			    URI[] files = context.getCacheFiles();

			    for (URI file : files) {

			        // read population.csv file data   
			        String line = ""; 

		            FileSystem fs = FileSystem.get(context.getConfiguration()); 
		            Path getFilePath = new Path(file.toString()); 

		            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

		            while ((line = reader.readLine()) != null) { 

		            	try {

			            	String[] params = line.split(",");

			                Text loc = new Text();
			                loc.set(params[1]);
			                long pop = Long.parseLong(params[4]);

			                country_pop.put(loc, pop);

			            } catch(NumberFormatException e) {
							System.out.println("Number Format Exception occured");
						} catch(ArrayIndexOutOfBoundsException e) {
							System.out.println("Array out of Bounds Exception occured");
						} 
		 				
		            }

			    }

		    } catch (IOException e) { 
	            System.out.println("Unable to read the File"); 
	            System.exit(1);
	        }

		}
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			
			try {

				float sum = 0;

				for (FloatWritable tmp: values)
					sum += tmp.get();
						
				float val = (sum*one_million)/country_pop.get(key);
				pop_per_1_mil.set(val);

				// This write to the final output
				context.write(key, pop_per_1_mil);

			} catch(NullPointerException e) {
				System.out.println("Null Pointer Exception Occured");
			} catch(ArithmeticException e) {
				System.out.println("Arithmetic Exception Occured");
			}
		}
	}
	
	
	public static void main(String[] args)  throws Exception {

		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "Distributed Cache");
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(FloatWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);

		myjob.addCacheFile(new Path(args[1]).toUri());

		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
