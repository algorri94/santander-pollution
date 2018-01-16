package es.unican.santander.pollution;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import static org.apache.storm.cassandra.DynamicStatementBuilder.*;

public class SantanderPollutionTopology {
    private static final String KAFKA_SPOUT = "KAFKA_SPOUT";
    private static final String PARSER_BOLT = "LINE_PARSER_BOLT";
    private static final String FILTER_BOLT = "FILTER_CLASSIFIER_BOLT";
    private static final String WLECTURAS_BOLT = "WRITE_LECTURAS_BOLT";
    private static final String WAGG_BOLT = "WRITE_AGGREGATION_BOLT";
    private static final String AGG_BOLT = "AGGREGATION_BOLT";
    private static final int[] regions = {0,1,2,3,4,5};

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("cassandra.keyspace", "santanderPollution");
        config.put("cassandra.port", "9042");

        TopologyBuilder builder = new TopologyBuilder();
        //Spout that reads data from Kafka
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(KafkaSpoutConfig.builder("127.0.0.1:" + "9092",
                                                                                    "pollution")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "stormPollution").build()), 1);
        //Bolt that parses the data received from Kafka
        builder.setBolt(PARSER_BOLT, new StringLineParser()).shuffleGrouping(KAFKA_SPOUT);
        //Bolt that filters the wrong results and classifies the data in different region locations
        builder.setBolt(FILTER_BOLT, new DataFilteringClassifier(), 1).shuffleGrouping(PARSER_BOLT);
        //Bolt that writes the filtered and classified data into Cassandra
        builder.setBolt(WLECTURAS_BOLT, new CassandraWriterBolt(async(
            simpleQuery("INSERT INTO lecturas (sensor_id, lat, lon, no2, ozone, temp, co, particles, region, generated, timeframe) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?);")
                .with(fields("id", "lat", "lon", "no2", "ozone", "temp", "co", "particles", "region", "generated", "timeframe"))
        )), 1).shuffleGrouping(FILTER_BOLT);
        //Bolt that aggregates the measures that belong to the same timeframe
        for(int r: regions) {
            builder.setBolt(AGG_BOLT+r, new TimeFrameAggregator(r), 1).shuffleGrouping(FILTER_BOLT);
            builder.setBolt(WAGG_BOLT+r,new CassandraWriterBolt(async(
                    simpleQuery("INSERT INTO pollutionAgg (no2, ozone, temp, co, particles, timeframe, region)" +
                            "VALUES (?,?,?,?,?,?,?);").with(fields("no2","ozone","temp","co","particles","timeframe","region"))
            )),1).shuffleGrouping(AGG_BOLT+r);
        }

        StormSubmitter.submitTopology("pollution", config, builder.createTopology());
        Thread.sleep(60000);
        System.exit(0);
    }
}
