package es.angelfrancisco.trainingstorm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {

    // Class attributes
    private String[] keywords;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    private SpoutOutputCollector spoutOutputCollector;

    // Class constructor. Receives an undefined sequence of strings
    public TwitterSpout(String... keywords) {
        this.keywords = keywords;
        this.queue = new LinkedBlockingQueue<Status>();
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        // Initialize attributes
        // TwitterStreamFactory contains twitter4j root configuration
        this.twitterStream = new TwitterStreamFactory().getInstance();
        this.spoutOutputCollector = spoutOutputCollector;

        // This variable defines the behavior of the Twitter stream listener
        StatusListener statusListener = new StatusListener() {
            // Behavior when a new status is received
            public void onStatus(Status status) {
                // Insert the status into the queue
                queue.offer(status);
            }

            // Behavior when
            public void onDeletionNotice(StatusDeletionNotice sdn) {
                // Do nothing
            }

            public void onTrackLimitationNotice(int i) {
                // Do nothing
            }

            public void onScrubGeo(long l, long l1) {
                // Do nothing
            }

            // Behavior when exception received
            public void onException(Exception e) {
                // Do nothing
            }

            // Behavior when stall warning is received
            public void onStallWarning(StallWarning warning) {
                // Do nothing
            }
        };

        // Set statusListener to the twitterStream object
        this.twitterStream.addListener(statusListener);
    }

    // Emits a new tuple to the topology
    public void nextTuple() {
        // Retrieves the first element in the queue
        Status status = this.queue.poll();

        // If no status, sleep 1 second; otherwise emits a tuple to the spoutOutputCollector
        if (status == null) {
            Utils.sleep(100);
        } else {
            this.spoutOutputCollector.emit(new Values(status));
        }
    }

    // Switch on spout
    public void activate() {
        // Start consuming public statuses that match with the keywords
        this.twitterStream.filter(this.keywords);
    };

    // Switch off spout
    public void deactivate() {
        // Shutdown internal stream consuming thread
        this.twitterStream.cleanUp();
    };

    // Close spout
    public void close() {
        // Shuts down internal dispatcher thread shared by all TwitterStream instances
        this.twitterStream.shutdown();
    }

    // Define spout output fields
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }
}