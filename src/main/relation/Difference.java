import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;


public class Difference {
	public static class DifferenceMap extends Mapper<LongWritable, Text, RelationA, Text>{
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws
		IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String relationName = fileSplit.getPath().getName();


			String s;
			s=relationName.toString();
			System.out.println("relationName: ");
			/*
			String[] records = new String(content.getBytes(),"UTF-8").split("\\n");
			for(int i = 0; i < records.length; i++){
				//Counter countPrint1 = context.getCounter("record", records[i]);
				//countPrint1.increment(1l);
				System.out.println("record: ");
				RelationA record = new RelationA(records[i]);
				context.write(record, relationName);
			 */
			Text t=new Text();
			t.set(relationName);
			RelationA record = new RelationA(line.toString());
			context.write(record, t);
		}
	}
	
	public static class DifferenceReduce extends Reducer<RelationA, Text, RelationA, NullWritable>{
		String setR;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			setR = context.getConfiguration().get("setR");
		}
		@Override
		public void reduce(RelationA key, Iterable<Text> value, Context context) throws 
		IOException,InterruptedException{
			for(Text val : value){
				if(!val.toString().equals(setR))
					return ;
			}
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job differenceJob = new Job();
		differenceJob.setJobName("differenceJob");
		differenceJob.setJarByClass(Difference.class);
		differenceJob.getConfiguration().set("setR", args[2]);
		
		differenceJob.setMapperClass(DifferenceMap.class);
		differenceJob.setMapOutputKeyClass(RelationA.class);
		differenceJob.setMapOutputValueClass(Text.class);

		differenceJob.setReducerClass(DifferenceReduce.class);
		differenceJob.setOutputKeyClass(RelationA.class);
		differenceJob.setOutputValueClass(NullWritable.class);

		differenceJob.setInputFormatClass(TextInputFormat.class);
		differenceJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(differenceJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(differenceJob, new Path(args[1]));
		
		differenceJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}