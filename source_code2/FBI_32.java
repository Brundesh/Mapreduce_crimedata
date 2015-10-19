
/*
 *Total  Number of cases investigated under FBI code 32
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
public class FBI_32 { 
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> { 

public void map(LongWritable key, Text value, Context context) 
throws IOException, InterruptedException { 
String line=value.toString();
if(line.length()>0 && line!=null){
String tokens[]=line.split(",");
if(tokens.length>11){
	
	String Fbicode=tokens[14];
	
	if(Fbicode.equals("32"))
		context.write(new Text(Fbicode), new IntWritable(1));
}
}
}
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> { 
public void reduce(Text key, Iterable<IntWritable> values, 
Context context) throws IOException, InterruptedException { 
	int count=0;
	for(IntWritable record:values){
		
			count++;
		
		
	}
	context.write(new Text("Number of crimes investigated for FBI code:"+key+" is"), new IntWritable(count));
	
}
}

public static void main(String[] args) throws Exception { 
Configuration conf = new Configuration(); 
Job job = new Job(conf, "wordcount"); 
job.setJarByClass(FBI_32.class);  
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
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
