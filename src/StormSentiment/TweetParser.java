/* Code for Apache Storm */
package StormSentiment;

import Classifier.TweetClassifier;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


class TweetParser extends BaseBasicBolt {

    TweetClassifier tc;

    public void prepare(Map conf, TopologyContext context){
        String abspath = (String) conf.get("classifier_path");
        try {
            tc = new TweetClassifier(abspath);
        }catch(Exception ex){
            System.out.println("Problem with classificator file");
            System.out.println(ex);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentiment"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        collector.emit(new Values(tc.evaluateText(tuple.getString(0))));
        System.out.println(tuple.getString(0));
    }


}
