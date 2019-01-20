/* Code for Apache Storm */

package StormSentiment;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;

class TweetRecord extends BaseBasicBolt {

    private static positiveCount = 0;
    private static negativeCount = 0;
    private static neutralCount = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        countSentiment(Integer.parseInt(tuple.getLong(0)));
    }

    private void countSentiment(int sentiment) {
        switch(sentiment) {
            case 0 :
                positiveCount++;
                break;
   
            case 1 :
                negativeCount++;
                break;
            default :
                neutralCount++;
        }

    }
}
