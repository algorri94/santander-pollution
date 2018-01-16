package es.unican.santander.pollution;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

public class TimeFrameAggregator implements IRichBolt {

    protected long currentFrame = 0;
    protected List<Tuple> tuples;
    protected OutputCollector collector;
    private int region;

    public TimeFrameAggregator(int region){
        this.region = region;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        tuples = new ArrayList<>();
        this.collector = collector;
    }

    public void execute(Tuple input) {
        if(region == input.getIntegerByField("region")) {
            long nextFrame = input.getLongByField("timeframe");

            if (currentFrame != 0 && currentFrame != nextFrame) {
                Map<String, Float> averages = calculateAverages(tuples);
                long timeframe = currentFrame;
                currentFrame = nextFrame;
                tuples.clear();
                collector.emit(tuple(averages.get("no2"), averages.get("ozone"), averages.get("temp"),
                        averages.get("co"), averages.get("particles"), timeframe, region));
            } else {
                currentFrame = nextFrame;
                tuples.add(input);
                collector.ack(input);
            }
        }
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("no2", "ozone", "temp", "co", "particles", "timeframe", "region"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private Map<String,Float> calculateAverages(List<Tuple> tuples) {
        float sum;
        Map<String,Float> values = new HashMap<String, Float>();
        String[] measures = {"no2","ozone","temp","co","particles"};
        for (String m : measures) {
            sum = 0;
            for (Tuple t : tuples) {
                sum += t.getFloatByField(m);
            }
            values.put(m, sum / tuples.size());
        }
        return values;
    }
}