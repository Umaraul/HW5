import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNFrequencies {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        Map<String,Integer> count = new HashMap<String,Integer>();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("\\d","").replaceAll("[^a-zA-Z ]",""));
            while (itr.hasMoreTokens()) {
                String wordDoc = itr.nextToken();
                //Dr. Farag's stop word list 
                if(wordDoc.equals("he")|| wordDoc.equals("she")|| wordDoc.equals("they")|| wordDoc.equals("the")|| 
                    wordDoc.equals("a")||wordDoc.equals("an") ||wordDoc.equals("are") ||wordDoc.equals("you") ||
                    wordDoc.equals("of") ||wordDoc.equals("is") ||wordDoc.equals("and") ||wordDoc.equals("or")) 
                {
                    continue;
                }
                //if already exists
                else if(count.containsKey(wordDoc)){
                    count.put(wordDoc,count.get(wordDoc)+1);
                //first appearance 
                }else {
                    count.put(wordDoc, 1); 
                }
            }
            //top 5
            for (Map.Entry<String, Integer> entry : count.entrySet()) {
                word.set(entry.getKey());
                IntWritable test = new IntWritable(entry.getValue());
                context.write(word, test);
            }
        }
    }
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator(){
            super(IntWritable.class,true);
        }
    }
    public int compare(WritableComparable w1, WritableComparable w2){
        IntWritable ip1 = (IntWritable) w1;
        IntWritable ip2 = (IntWritable) w2;
        return w1.compareTo(w2);
    }



    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        TreeMap<Integer,String> top = new TreeMap<Integer,String>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            top.put(sum,key.toString());
            if(top.size() > 5){
                top.remove(top.firstKey()); 
            }
        }
        //cleaning up to send top 5
        public void cleanup (Context context) throws IOException, InterruptedException
        {

            for(Map.Entry<Integer,String> entry: top.entrySet()){
                IntWritable count = new IntWritable(entry.getKey());
                Text word = new Text(entry.getValue());
                context.write(word,count);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top N Frequency");
        job.setJarByClass(TopNFrequencies.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}