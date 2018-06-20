import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class RelativeFreq {
	public static class map1 extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] elements = line.split(" ");
			IntWritable iw = new IntWritable(1);
			String word1="";
			String word2="";
			for(int i = 0; i < elements.length; i++ ){
				if(i<elements.length-1)
				{
					word1=elements[i];
					word2=elements[i+1];
					if(!word1.endsWith(".")){
						context.write(new Text (word1+"\t"+word2), iw);
						context.write(new Text(word1), iw);
					}
					else
					{
						context.write(new Text (word1), iw);
					}
					
			}
					
			}
			
		}	
		}
	
	public static class Reduce1 extends
	Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable sv = new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException {
	int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    sv.set(sum);
			context.write(key, sv);
			
		}
	}
	
	public static class RelativeMap extends Mapper<Text, Text, Text,Text> 
	{
		public void map(Text Keyname,Text Values, Context context) throws IOException, InterruptedException {
			String[] wordname=Keyname.toString().split(",");
			if(wordname.length==1)
			{
				context.write(new Text(wordname[0]), new Text(Values));	
								
			}else if(wordname.length==2)
			{
				context.write(new Text(wordname[0]), new Text(wordname[1]+"\t"+Values));
			}
		}
	}
	
	public static class RelativeReduce extends Reducer<Text,Text,Double,Text>
	{
	public void reduce(Text Keyname,Iterable<Text> Values, Context context) throws IOException, InterruptedException {
		double rf = 0;
		double count=0;
		//long differencecount=0;
		String[] wordname=Keyname.toString().split("\t");
		List<Text> elements=new ArrayList<Text>();
		for(Text value:Values)
		{
			elements.add(new Text(value));
		}
		for(int i=0;i<elements.size();i++)
		{
			String[] elements1=elements.get(i).toString().split("\t");
			//if(elements1.length==2)
			//{
			//	count = count+Integer.parseInt(elements1[1]);
				//differencecount = count-Integer.parseInt(elements1[1]);
				
			//}
			if(elements1.length==1){
				count = count+Integer.parseInt(elements1[0]);
				//context.write(count, new Text(wordname[0]));
			}
		}
		for(int i=0;i<elements.size();i++)
		{
			
			String[] elements2=elements.get(i).toString().split("\t");
			if(elements2.length==2)
			{
				if(count>Integer.parseInt(elements2[1])){
				rf = (double)((Integer.parseInt(elements2[1])/(count-Integer.parseInt(elements2[1]))));
				rf=Math.round(rf*100)/100.00;
				context.write(rf,new Text(wordname[0] +" "+ elements2[0]));
			}}
			if(elements2.length==1){
				if(count>Integer.parseInt(elements2[0])){
				rf = (double)((Integer.parseInt(elements2[0])/(count-Integer.parseInt(elements2[0]))));
				rf=Math.round(rf*100)/100.00;
				context.write(rf,new Text(wordname[0]));
				}
			}
		}
		
	}
		
	}
	
	public static class SorttopMap extends Mapper<LongWritable, Text, DoubleWritable,Text> 
	{ 
		public void map(LongWritable Key,Text Value, Context context) throws IOException, InterruptedException 
		{
		String[] elements=Value.toString().split("\t");
	    Double rf=Double.parseDouble(elements[0].toString());
		context.write(new DoubleWritable(rf) ,new Text(elements[1]));
	}
}
	
	public static class SorttopReduce extends Reducer<Text,Text,DoubleWritable,Text>
	{
		
		public void reduce(DoubleWritable Key,Iterable<Text> Values, Context context) throws IOException, InterruptedException 
		{
           // int counter = 0;
            for(Text value:Values) {
              //  if (counter == 50) {
              //      break;
              //  }
                context.write(Key, value);
            //   counter++;
            }	
			}
	}
	
	public static class top100Map extends Mapper<Text, Text, Text,Text> 
	{ 
		List<Text> elements=new ArrayList<Text>();
	//int counter=0;
		public void map(Text Key,Text Value, Context context) throws IOException, InterruptedException 
		{

          //      if (counter == 50) {
          //          break;
           //     }
            
             context.write(Key, Value);
               // counter++;
			

	}
	}
	
	
	public static class top100Reduce extends Reducer<Text,Text,Text,Text>
	{
		List<Text> value=new ArrayList<Text>();
	
		int mCount = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mCount = 0;
		}
		public void reduce(Text Key,Iterable <Text> Values, Context context) throws IOException, InterruptedException 
		{
		Text t = new Text("1.0");
			String line=Values.toString();
			if(mCount < 100) {
					for(Text value:Values) {
						//String[] values=line.split("\t");
						//context.write(Key, new Text(values[0]+" "+values[1]));
						context.write(value,new Text(Key +" = "+t));
						mCount++;
	                                   //     if(mCount > 100) {
	                                   //         break;
	                                   //    }
					}
		}

	}
		
	}

	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Split & Count");
		job.setJarByClass(RelativeFreq.class); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		job.setMapperClass(map1.class);
		job.setReducerClass(Reduce1.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/wikioutput"));
		job.waitForCompletion(true);
		
		Job job1 = new Job(conf, "Relative Freq");
		job1.setJarByClass(RelativeFreq.class); 
		job1.setMapOutputKeyClass(Text.class); 
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(RelativeMap.class);
		job1.setReducerClass(RelativeReduce.class);
		job1.setNumReduceTasks(1);
		job1.setInputFormatClass(KeyValueTextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path("/wikioutput"));
		FileOutputFormat.setOutputPath(job1, new Path("/wikioutput1"));
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "Sorting");
		job2.setJarByClass(RelativeFreq.class); 
		job2.setMapOutputKeyClass(DoubleWritable.class); 
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(SorttopMap.class);
		job2.setReducerClass(SorttopReduce.class);
		job2.setNumReduceTasks(1);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path("/wikioutput1"));
		FileOutputFormat.setOutputPath(job2, new Path("/wikioutput2"));
		job2.waitForCompletion(true);
		
		Job job3 = new Job(conf, "Top100");
		job3.setJarByClass(RelativeFreq.class); 
		job3.setMapOutputKeyClass(Text.class); 
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class); 
		job3.setOutputValueClass(Text.class);
		job3.setMapperClass(top100Map.class);
		job3.setReducerClass(top100Reduce.class);
		job3.setNumReduceTasks(1);
		job3.setInputFormatClass(KeyValueTextInputFormat.class); 
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setSortComparatorClass(DecreasingComparator.class);
		FileInputFormat.addInputPath(job3, new Path("/wikioutput2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		job3.waitForCompletion(true);

	}
	

	
}
