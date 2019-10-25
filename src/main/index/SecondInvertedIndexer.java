
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.google.inject.internal.cglib.reflect.$FastMember;

public class SecondInvertedIndexer {
    /** 自定义FileInputFormat **/
    public static class FileNameInputFormat extends FileInputFormat<Text, Text> {
        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) throws IOException, InterruptedException {

            FileNameRecordReader fnrr = new FileNameRecordReader();
            fnrr.initialize(split, context);
            return fnrr;
        }
    }

    /** 自定义RecordReader
     * key: filename  value: line recorder value**/
    public static class FileNameRecordReader extends RecordReader<Text, Text> {
        String fileName;
        LineRecordReader lrr = new LineRecordReader();

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(fileName);
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return lrr.getCurrentValue();
        }

        @Override
        public void initialize(InputSplit arg0, TaskAttemptContext arg1)
                throws IOException, InterruptedException {
            lrr.initialize(arg0, arg1);
            fileName = ((FileSplit) arg0).getPath().getName();
        }

        public void close() throws IOException {
            lrr.close();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            return lrr.nextKeyValue();
        }

        public float getProgress() throws IOException, InterruptedException {
            return lrr.getProgress();
        }
    }


    public static class InvertedIndexMapper extends
            Mapper<Text, Text, CombinationKey, Text> {
        private Set<String> stopwords;
        private Path[] localFiles;
        private String pattern = "[^\\w]"; // 正则表达式，代表不是0-9, a-z, A-Z,的所有其它字

        public void setup(Context context) throws IOException, InterruptedException {
            System.out.println("mapper is setting up");
            stopwords = new TreeSet<String>();
            Configuration conf = context.getConfiguration();
            localFiles = DistributedCache.getLocalCacheFiles(conf); // 获得停词表
            for (int i = 0; i < localFiles.length; i++) {
                String line;
                BufferedReader br =
                        new BufferedReader(new FileReader(localFiles[i].toString()));
                while ((line = br.readLine()) != null) {
                    StringTokenizer itr = new StringTokenizer(line);
                    while (itr.hasMoreTokens()) {
                        stopwords.add(itr.nextToken());
                    }
                }
            }
        }

        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            // map()函数这里使用自定义的FileNameRecordReader
            // 得到key: filename文件名; value: line_string每一行的内容
            String temp = new String();
            String line = value.toString().toLowerCase();
            line = line.replaceAll(pattern, " "); // 将非0-9, a-z, A-Z的字符替换为空格
            StringTokenizer itr = new StringTokenizer(line);
            for (; itr.hasMoreTokens();) {
                temp = itr.nextToken();
                if (!stopwords.contains(temp)) {
                    CombinationKey ck=new CombinationKey();
                    ck.setFirstKey(temp);
                    ck.setSecondKey(0);
                    context.write(ck, new Text(key.toString()+"-"+"1"));//word: word  value: filename-1
                }
            }
        }
    }

    /** 使用Combiner将Mapper的输出结果中value部分的词频进行统计
     * 输出为word,count   doc**/
    public static class SumCombiner extends
            Reducer<CombinationKey, Text, CombinationKey, Text> {

        public void reduce(CombinationKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String,Integer> docList = new HashMap<String, Integer>();
            String doc;
            int num=0;
            int sum=0;
            for(Text t:values){
                doc=t.toString().split("-")[0];
                num=Integer.parseInt(t.toString().split("-")[1]);
                sum=sum+num;
                if(!docList.containsKey(doc)){
                    docList.put(doc,num);
                }
                else{
                    docList.put(doc,docList.get(doc)+num);
                }
            }

            for(String dockey : docList.keySet()){
                int thisDocNum = docList.get(dockey);

                String valTmp=dockey+","+thisDocNum;

                CombinationKey ck=new CombinationKey();

                ck.setFirstKey(key.getFirstKey());
                ck.setSecondKey(sum);//所有doc下的总数(bushi

                System.out.println("word: "+ck.getFirstKey()+"total: "+ck.getSecondKey());
                context.write(ck, new Text(valTmp));//output key <word, total sum>    value: doc, thisDocNum
            }

        }
    }

    /** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer
     * 注意这并不会改变map发送到reducer的键值对数据和类型 **/
    public static class NewPartitioner extends HashPartitioner<CombinationKey, Text> {
        //override partition 方法
        public int getPartition(CombinationKey key, Text value, int numReduceTasks) {
            //String term = new String();
            // term = key.getFirstKey(); // <term#docid>=>term
            //getpartition 返回0到reducer数目中间的整型数来确定将<key,value>键值对发到哪一个reducer中
            //return super.getPartition(new Text(term), value, numReduceTasks);这样不行， 此时super方法中形参要求CombinationKey
            return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numReduceTasks;
        }
    }


    /**一个reduce会收到的内容：1.大部分为同一个term 2.少部分为其他term
     * 在InvertedIndexReducer中，根据不同的key(word#docid)调用多次reduce
     * curretnItem用于判断当前key中的word是否和上次调用的key中的word相同
     * postingList用于存储整合多次key-word相同的reduce调用的过程中的value**/

    public static class InvertedIndexReducer extends
            Reducer<CombinationKey, Text, Text, Text> {
        private Text word1 = new Text();
        private Text word2 = new Text();

        int docSum=0;
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(CombinationKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            word1.set(key.getFirstKey());//word


            if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {

                StringBuilder out = new StringBuilder();
                long count = 0;
                for (String p : postingList) {
                    out.append(p);
                    out.append(";");
                    count =
                            count
                                    + Long.parseLong(p.substring(p.indexOf(",") + 1,
                                    p.indexOf(">")));
                }
                out.append("<total," + count + ">.");
                if (count > 0)
                    context.write(CurrentItem, new Text(out.toString()));
                postingList = new ArrayList<String>();
            }

            CurrentItem = new Text(word1);
            for(Text t:values){
                word2.set("<" + t.toString() +">");//filename, sum
                postingList.add(word2.toString()); // 不断向postingList也就是文档名称中添加词表
            }

        }

        // cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            StringBuilder out = new StringBuilder();
            long count = 0;
            for (String p : postingList) {
                out.append(p);
                out.append(";");
                count =
                        count
                                + Long
                                .parseLong(p.substring(p.indexOf(",") + 1, p.indexOf(">")));
            }
            out.append("<total," + count + ">.");
            if (count > 0)
                context.write(CurrentItem, new Text(out.toString()));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI(
                "hdfs://h0:8010/user/root/stop-words.txt"), conf);// 设置停词列表文档作为当前作业的缓存文件
        Job job = new Job(conf, "inverted index");
        job.setJarByClass(InvertedIndexer.class);
        job.setInputFormatClass(FileNameInputFormat.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);


        job.setSortComparatorClass(CustomComparator.class);

        job.setMapOutputKeyClass(CombinationKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


