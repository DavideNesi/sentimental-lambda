/* Code for Apache Storm */
package StormSentiment;

import org.apache.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import Classifier.TweetClassifier;

public class TweetTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweet_spout", new StormSentiment.TweetSpout(), 4);

        builder.setBolt("tweet_parser", new TweetParser(), 4).shuffleGrouping("tweet_spout");

        builder.setBolt("tweet_recorder", new StormSentiment.TweetRecord(), 4).fieldsGrouping("tweet_parser", new Fields("sentiment"));

        // Create classfier
        TweetClassifier tc = new TweetClassifier(args[0]);

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.set("classifier", tc);

        cluster.submitTopology("tweet-sentiment", conf, builder.createTopology());

        Thread.sleep(10000);

        cluster.shutdown();
    }
}
