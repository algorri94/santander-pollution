package es.unican.santander.pollution;

import static org.apache.storm.utils.Utils.tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class StringLineParser implements IBasicBolt {

    private static String splitChar = ",";

    public void prepare(Map stormConf, TopologyContext context) {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = (String) input.getValue(4);
        String[] fields = line.split(splitChar);
        int id = Integer.parseInt(fields[0]);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
        long timestamp = -1;
        try {
            timestamp = formatter.parse(fields[1]).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        float lat = Float.parseFloat(fields[3]);
        float lon = Float.parseFloat(fields[4]);
        int no2 = Integer.parseInt(fields[5]);
        int ozone = Integer.parseInt(fields[6]);
        float temp = Float.parseFloat(fields[9]);
        float co = Float.parseFloat(fields[10]);
        float particles = Float.parseFloat(fields[11]);
        collector.emit(tuple(id, lat, lon, no2, ozone, temp, co, particles, timestamp));
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "lat", "lon", "no2", "ozone", "temp", "co", "particles", "generated"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}