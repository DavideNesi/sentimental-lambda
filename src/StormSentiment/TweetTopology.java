/* Code for Apache Storm */
package StormSentiment;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TweetTopology {

    LocalCluster cluster;

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        HdfsSpout textReaderSpout = new HdfsSpout().withOutputFields("line")
                                                    .setHdfsUri("hdfs://localhost:9000")  // required
                                                    .setSourceDir("/storm/input")          // required
                                                    .setArchiveDir("/storm/processed")     // required
                                                    .setBadFilesDir("/storm/badfiles")
                                                    .setReaderType("org.apache.storm.hdfs.spout.TextFileReader");

        /* File that doesn't have .ignore extension will be processed */

        builder.setSpout("tweet_spout", textReaderSpout, 4);

        builder.setBolt("tweet_parser", new TweetParser(), 4).shuffleGrouping("tweet_spout");

        builder.setBolt("tweet_recorder", new CassandraBolt(), 4).fieldsGrouping("tweet_parser", new Fields("sentiment"));

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.put("classifier_path", args[0]);
        conf.put("cassandra_address", args[1]);
        if (args.length > 2)
            conf.put("keyword", args[2]);

        cluster.submitTopology("tweet-sentiment", conf, builder.createTopology());

        //Thread.sleep(10000);
        //cluster.shutdown();
    }

    public void shutdownCluster(){ this.cluster.shutdown();}
}
