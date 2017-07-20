package pack;

import java.io.IOException; 
import java.util.Iterator; 
import java.util.StringTokenizer; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat; 
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.mapred.TextInputFormat; 

 
public class CompressionWithGzip { 
	
	
 
    public static class Map extends MapReduceBase implements 
            Mapper<LongWritable, Text, Text, IntWritable> { 


    	Text k= new Text(); 

    	
        public void map(LongWritable key, Text value, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException { 

        		
                String line = value.toString(); 
                StringTokenizer tokenizer = new StringTokenizer(line," "); 
 
                while (tokenizer.hasMoreTokens()) { 
                	String year= tokenizer.nextToken();
            	    k.set(year);
            	
            	
            	    String temp= tokenizer.nextToken().trim();

            	       	
            	int v = Integer.parseInt(temp); 

            	
                output.collect(k,new IntWritable(v)); 
            } 
        } 
    } 
 
 
    
    public static class Reduce extends MapReduceBase implements 
            Reducer<Text,IntWritable, Text, IntWritable> { 


        public void reduce(Text key, Iterator<IntWritable> values, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
           

        	int maxtemp=0;
            
        	
        	while (values.hasNext()) { 
  
        		int temperature= values.next().get(); 
            
            	if(maxtemp<temperature)
            	{
            		maxtemp =temperature;
            	}
            } 
            
            
            output.collect(key, new IntWritable(maxtemp)); 
        } 
 
    } 

    public static void main(String[] args) throws Exception { 
 
        JobConf conf = new JobConf(CompressionWithGzip.class); 
        conf.setJobName("temp"); 
 
        conf.setOutputKeyClass(Text.class); 
        conf.setOutputValueClass(IntWritable.class); 
 
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
       
        conf.setMapperClass(Map.class); 
        conf.setReducerClass(Reduce.class); 
 
        conf.setInputFormat(TextInputFormat.class); 

        conf.setCompressMapOutput(true);
        
        
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);

        
        FileInputFormat.setInputPaths(conf, new Path(args[0])); 
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
        
 
        JobClient.runJob(conf); 

		
	}
	
}