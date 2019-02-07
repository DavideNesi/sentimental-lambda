package HadoopSentiment;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Classifier.TweetClassifier;
import CassandraAdapter.CassandraDriver;

import javax.naming.Context;

public class SentimentMapReduce extends Configured implements Tool {

    private static Random randnum = new Random();
    private String[] args;

    public static class Filter extends Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /* Get a string (value), parse it in two pieces (ID and text)*/

            Configuration conf = context.getConfiguration();
            String query = conf.get("query"); // can return null

            // Fetch line and split in 4 chunks
            String line = value.toString();
            String values[] = line.split(",", 4);
            int tweetID = Integer.valueOf(values[0]);
            String tweetText = values[3];

            if(evalQuery(tweetText, query))
                context.write(new LongWritable(tweetID), new Text(tweetText));
        }

        public static boolean evalQuery(String text, String query){
            if (query == null)  // no filter, every text is ok
                return true;

            return text.contains(query);
        }

    }

    public static class Sentiment extends Mapper<LongWritable, Text, Text, IntWritable> {

        private TweetClassifier tc = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                TweetClassifier tc = new TweetClassifier(context.getConfiguration().get("classifier"));
            }catch(Exception ex){ System.out.println(ex); }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(tc.evaluateText(value.toString())), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private CassandraDriver cassandra = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                cassandra = new CassandraDriver();
                cassandra.createConnection(context.getConfiguration().get("cassandra_ip")); // Ip address of Cassandra cluster
            }catch(Exception ex){ System.out.println(ex); }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            context.write(key, new IntWritable(sum));

            try{
                String query = context.getConfiguration().get("query");
                cassandra.addTweetCountEntry(query, key.toString(), sum);
            }catch (Exception ex){
                System.out.println("I was unable to write on Cassandra. Check log or stack trace.");
                System.out.println(ex);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(cassandra != null)
                try{
                    cassandra.closeConnection();
                }catch (Exception ex){ System.out.println(ex); }
        }
    }

    public int run(String[] args) throws Exception {


        Configuration conf = getConf();

        System.out.println("Input dir: " + args[0]);
        System.out.println("Output dir: " + args[1]);

        Job job = Job.getInstance(conf, "TwitterSentiment");

        Configuration chainMapConf = new Configuration(false);
        if (args.length >3)
            chainMapConf.set("query", args[3]);

        chainMapConf.set("classifier", args[2]);

        conf.set("cassandra_ip", "127.0.0.1");
        if (args.length >4)
            conf.set("cassandra_ip", args[4]);

        ChainMapper.addMapper(job, Filter.class, Object.class, Text.class, LongWritable.class, Text.class, chainMapConf);
        ChainMapper.addMapper(job, Sentiment.class, LongWritable.class, Text.class, Text.class, IntWritable.class, chainMapConf);

        job.setJarByClass(SentimentMapReduce.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SentimentMapReduce(), args);
        System.exit(res);
    }
}
