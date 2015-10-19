/*
 *Number of arrests in theft case district wise.
 * */
import java.io.IOException; 
import java.util.*; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
public class District_wise_Crimes { 
public static class Map extends Mapper<LongWritable, Text, Text, Text> { 

public void map(LongWritable key, Text value, Context context) 
throws IOException, InterruptedException { 
String line=value.toString();
if(line.length()>0 && line!=null){
String tokens[]=line.split(",");
if(tokens.length>11){
	String district=tokens[11];
	String Crime_type=tokens[5];
	String arrest=tokens[8];
	
	context.write(new Text(district), new Text(Crime_type+","+arrest));
}
}
}
}
public static class Reduce extends Reducer<Text, Text, Text, IntWritable> { 
public void reduce(Text key, Iterable<Text> values, 
Context context) throws IOException, InterruptedException { 
	int count=0;
	for(Text record:values){
		String line=record.toString();
		String parts[]=line.split(",");
		if(parts[0].contains("THEFT")&&parts[1].equals("true")){
			count++;
		}
		
	}
	context.write(new Text("Total  arrest for theft in:"+key), new IntWritable(count));
	
}
}

public static void main(String[] args) throws Exception { 
Configuration conf = new Configuration(); 
Job job = new Job(conf, "wordcount"); 
job.setJarByClass(District_wise_Crimes.class);  
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);  
job.setOutputValueClass(IntWritable.class);  
job.setMapperClass(Map.class);
//job.setNumReduceTasks(0);
job.setReducerClass(Reduce.class); 
 
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class); 

FileInputFormat.addInputPath(job, new Path(args[0])); 
FileOutputFormat.setOutputPath(job,new Path(args[1]));
Path out=new Path(args[1]);
out.getFileSystem(conf).delete(out);
job.waitForCompletion(true); 
} 
}
