package es.angelfrancisco.trainingstorm;

import es.angelfrancisco.trainingstorm.bolt.AnalyzeTweetsBolt;
import es.angelfrancisco.trainingstorm.bolt.ExtractMentionsBolt;
import es.angelfrancisco.trainingstorm.spout.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {

    public static void main(String[] args) {
        // If no args show message and exit
        if (args.length==0) {
            System.out.println("You must introduce at least one keyword");

            return;
        }

        // Create topology object
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // Add spouts and bolts to the topology
        topologyBuilder.setSpout("twitter-spout", new TwitterSpout(args));
        topologyBuilder.setBolt("count-characters", new AnalyzeTweetsBolt()).shuffleGrouping("twitter-spout");
        topologyBuilder.setBolt("extract-mentions", new ExtractMentionsBolt()).shuffleGrouping("count-characters", "status");

        // Define cluster configuration
        // With setDebug(true) the output console will show Storm debug log
        Config config = new Config();
        config.setDebug(true);

        // Create Storm local cluster and send it the topology created to being run
        // This LocalCluster simulates a Storm cluster in a local/development environment
        final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("sample-twitter-topology", config, topologyBuilder.createTopology());
    }
}