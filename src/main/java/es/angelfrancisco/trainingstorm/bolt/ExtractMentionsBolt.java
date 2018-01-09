package es.angelfrancisco.trainingstorm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

import java.util.Map;

public class ExtractMentionsBolt extends BaseRichBolt {

    // Class attributes
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // Do nothing
    }

    // Bolt initialization
    public void execute(Tuple tuple) {
        // The logic of this bolt is the store (in this case, print) the tweet id and it associated mentions

        Status status = (Status) tuple.getValueByField("status");

        int mentions = status.getUserMentionEntities().length;
        long id = status.getId();

        // Here the info should be saved into the database
        // In this example, the data only printed
        System.out.println(String.format("The tweet %d with more than 140 characters has %s mentions", id, (mentions==0) ? "no" : mentions));
    }

    // Define bolt output fields
    // In this case is the last bolt, so no action required
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Do nothing
    }
}
