import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * ????к??id????????????value?????
 * @author KING
 *
 */
public class Selection {
	public static class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable>{
		private String col;
		private String value;
		private char opt;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			col = context.getConfiguration().get("col");
			value = context.getConfiguration().get("value");
			opt = context.getConfiguration().get("opt").charAt(0);//转为char
		}
		
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws
		IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			if(record.isCondition(col,opt, value))
				context.write(record, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job selectionJob = new Job();
		selectionJob.setJobName("selectionJob");
		selectionJob.setJarByClass(Selection.class);
		selectionJob.getConfiguration().set("col", args[2]);
		selectionJob.getConfiguration().set("opt", args[3]); //操作符号
		selectionJob.getConfiguration().set("value", args[4]);
		
		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(RelationA.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);

		selectionJob.setNumReduceTasks(0);

		selectionJob.setInputFormatClass(TextInputFormat.class);
		selectionJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
		
		selectionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
