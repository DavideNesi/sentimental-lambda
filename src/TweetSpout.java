/* Code for Apache Storm */

package StormSentiment;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class TweetSpout extends BaseRichSpout {

    private static final Random rand = new Random();
    private static final DateTimeFormatter isoFormat =
            ISODateTimeFormat.dateTimeNoMillis();

    private SpoutOutputCollector collector;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public void nextTuple() {
        Utils.sleep(rand.nextInt(100)); // using this one to randomize
        String line = isoFormat.print(DateTime.now()) + "This is a tweet"; //TODO replace with real db access
        collector.emit(new Values(line));
    }
}
