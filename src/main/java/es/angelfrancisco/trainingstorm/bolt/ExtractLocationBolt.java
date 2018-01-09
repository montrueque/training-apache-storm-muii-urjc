package es.angelfrancisco.trainingstorm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import twitter4j.User;

import java.util.Map;

public class ExtractLocationBolt extends BaseRichBolt {

    // Class attributes
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // Do nothing
    }

    // Bolt logic
    public void execute(Tuple tuple) {
        // The logic of this bolt is:
        // If the location of the user is the same of a predefined location, this user is saved (print)

        User user = (User) tuple.getValueByField("user");
        String userLocation = user.getLocation();
        String locationToCompare = "USA";

        if ((userLocation != null) &&(userLocation.toLowerCase().contains(locationToCompare.toLowerCase()))) {
            // Here the info should be saved into the database
            // In this example, the data only printed
            System.out.println(String.format("The verified user %s has its location in %s", user.getName(), userLocation));
        }
    }

    // Define bolt output fields
    // In this case is the last bolt, so no action required
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Do nothing
    }
}
