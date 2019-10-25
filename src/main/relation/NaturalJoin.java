import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 自然连接操作,在属性col上进行连接
 * @author KING
 *
 */
public class NaturalJoin {
	public static class NaturalJoinMap extends Mapper<LongWritable, Text, Text, Text>{
		private int col;
		private String relationNameA;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			col = context.getConfiguration().getInt("col", 0);
			relationNameA = context.getConfiguration().get("relationNameA");
		}
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws
		IOException, InterruptedException{
			//String s= "";
			//s=relationName.toString();
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String relationName = fileSplit.getPath().getName();

			//更好的做法是使用子类型，多态，但是getCol方法是在RelationA和RelationB中分别实现的，且二者并没有继承一个共同的Relation基类，故放弃
			if(relationName.equalsIgnoreCase(relationNameA)){
				RelationA record = new RelationA(line.toString());
				Text attr=new Text(record.getCol(col));//自然连接属性的值
				Text combine=new Text(relationName+"-"+line.toString());
				context.write(attr, combine);
			}
			else{
				RelationB record = new RelationB(line.toString());
				Text attr=new Text(record.getCol(col));//自然连接属性的值
				Text combine=new Text(relationName+"-"+line.toString());
				context.write(attr, combine);
				//在reduce里面新建一个relationA,并且设置相关的值就可以了
			}
		}
	}


	public static class NaturalJoinReduce extends Reducer<Text,Text,Text,NullWritable> {
		private String relationNameA;
		private int col;

		protected void setup(Context context) throws IOException, InterruptedException {
			relationNameA = context.getConfiguration().get("relationNameA");
			col = context.getConfiguration().getInt("col", 0);
		}

		public void reduce(Text key, Iterable<Text> value, Context context) throws
				IOException, InterruptedException {


			ArrayList<RelationA> setA = new ArrayList<RelationA>();
			ArrayList<RelationB> setB = new ArrayList<RelationB>();
			for (Text val : value) {
				String[] recordInfo = val.toString().split("-");
				if (recordInfo[0].equalsIgnoreCase(relationNameA))
					setA.add(new RelationA(recordInfo[1].toString()));
				else
					setB.add(new RelationB(recordInfo[1].toString()));

			}
			//做笛卡尔乘积
			for (int i = 0; i < setA.size(); i++) {
				for (int j = 0; j < setB.size(); j++) {
					Text t = new Text(setA.get(i).toString() + "," + setB.get(j).getValueExcept(col));
					context.write(t, NullWritable.get());
				}

			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job naturalJoinJob = new Job();
		naturalJoinJob.setJobName("naturalJoinJob");
		naturalJoinJob.setJarByClass(NaturalJoin.class);
		naturalJoinJob.getConfiguration().setInt("col", Integer.parseInt(args[2]));
		naturalJoinJob.getConfiguration().set("relationNameA", args[3]);
		
		naturalJoinJob.setMapperClass(NaturalJoinMap.class);
		naturalJoinJob.setMapOutputKeyClass(Text.class);
		naturalJoinJob.setMapOutputValueClass(Text.class);

		naturalJoinJob.setReducerClass(NaturalJoinReduce.class);
		naturalJoinJob.setOutputKeyClass(Text.class);
		naturalJoinJob.setOutputValueClass(NullWritable.class);

		naturalJoinJob.setInputFormatClass(TextInputFormat.class);
		naturalJoinJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(naturalJoinJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(naturalJoinJob, new Path(args[1]));
		
		naturalJoinJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
