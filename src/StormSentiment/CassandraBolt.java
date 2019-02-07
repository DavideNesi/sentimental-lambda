package StormSentiment;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;
import CassandraAdapter.CassandraDriver;

class CassandraBolt extends BaseBasicBolt {

    private static int positiveCount = 0;
    private static int negativeCount = 0;
    private static int neutralCount = 0;

    private static final int THRESHOLD = 100;

    private String keyword = null;

    private CassandraDriver cassandra;

    public void prepare(Map stormConf, TopologyContext context) {
        keyword = (String) stormConf.get("keyword"); // might be null

        try{
            cassandra = new CassandraDriver();
            cassandra.createConnection((String)stormConf.get("cassandra_address")); // Ip address of Cassandra cluster
        }catch (Exception ex) {
            System.out.println("ERROR: Couldn't reach Cassandra");
            System.out.println(ex);
            cassandra = null;
        }
    }

    public void cleanup() {
        System.out.println(positiveCount + " " + negativeCount + " "+ neutralCount);

        if(positiveCount + negativeCount + neutralCount != 0)
            updateTweets();

        if(cassandra != null)
            cassandra.closeConnection();
    }

    private void updateTweets(){
        long results[] = cassandra.getTweetCountByKeywordStorm(keyword); // if not found returns {0, 0, 0}

        results[0] += positiveCount;
        results[1] += negativeCount;
        results[2] += neutralCount;

        cassandra.addTweetCountStorm(keyword, results);

        positiveCount = 0;
        negativeCount = 0;
        neutralCount = 0;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        countSentiment(tuple.getString(0));

        if (positiveCount + negativeCount + neutralCount > THRESHOLD)
            updateTweets();
    }

    private void countSentiment(String sentiment) {
        if (sentiment.contains("pos")) {
            positiveCount++;
        }else if(sentiment.contains("neg")) {
            negativeCount++;
        }else{
            neutralCount++;
        }

    }

}