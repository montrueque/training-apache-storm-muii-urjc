package es.angelfrancisco.trainingstorm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.User;

import java.util.Map;

public class AnalyzeTweetsBolt extends BaseRichBolt {

    // Class attributes
    private OutputCollector outputCollector;

    // Bolt initalization
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    // Bolt logic
    public void execute(Tuple tuple) {
        // The logic of this bolt is:
        // If tweet has more than 140 characters emit in stream "status"
        // If tweet has less or equal than 140 characters and user is verified emit in stream "user"

        // From the tuple received want to get status and its user
        Status status = (Status) tuple.getValueByField("status");
        User user = status.getUser();

        if (status.getText().length() > 140) {
            this.outputCollector.emit("status", new Values(status));
        }
        else if (user.isVerified()) {
            this.outputCollector.emit("user", new Values(user));
        }
    }

    // Define bolt output fields
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("status", new Fields("status"));
        outputFieldsDeclarer.declareStream("user", new Fields("user"));
    }
}
