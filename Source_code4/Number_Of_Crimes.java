/*
 *Total  Number arrests happened  between 10/2014 to 10/2015
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
public class Number_Of_Crimes { 
public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> { 

public void map(LongWritable key, Text value, Context context) 
throws IOException, InterruptedException { 
String line=value.toString();
if(line.length()>0 && line!=null){
String tokens[]=line.split(",");
if(tokens.length>2){
String date=tokens[2];
int month=Integer.parseInt(date.substring(0,2));
int year=Integer.parseInt(date.substring(6,10));
if(year==2014){
	if(month>=10)
		context.write(NullWritable.get(), value);
}else if(year==2015){
	if(month<=10)
		context.write(NullWritable.get(), value);
}
}
}
}
}
public static class Reduce extends Reducer<NullWritable, Text, Text, IntWritable> { 
public void reduce(NullWritable key, Iterable<Text> values, 
Context context) throws IOException, InterruptedException { 
	int count=0;
	for(Text record:values){
		String line=record.toString();
		String parts[]=line.split(",");
		
		if(parts[8].equals("true")){
			count++;
		}
	}
context.write(new Text("Number of Arrest"), new IntWritable(count));
}
}
public static void main(String[] args) throws Exception { 
Configuration conf = new Configuration(); 
Job job = new Job(conf); 
job.setJarByClass(Number_Of_Crimes.class);  
job.setMapOutputKeyClass(NullWritable.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);  
job.setOutputValueClass(IntWritable.class);  
job.setMapperClass(Map.class);  
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