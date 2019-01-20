/* Code for Apache Storm */
package StormSentiment;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import sentiment-lambda.TweetClassifier;

class TweetParser extends BaseBasicBolt {
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentiment"));
    }
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        collector.emit(new Values(getSentimentQuality(tuple.getString(0))));
    }

    public static int getSentimentQuality (String comment){
        return randnum.nextInt(2);//TODO it for real
    }

}
