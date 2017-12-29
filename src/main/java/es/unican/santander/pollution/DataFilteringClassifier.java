package es.unican.santander.pollution;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

public class DataFilteringClassifier implements IBasicBolt {

    private static String splitChar = ",";
    private static final int AggregateFrequency = 10 * 1000; //10s

    public void prepare(Map stormConf, TopologyContext context) {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        int id = (int) input.getValue(0);
        float lat = (float) input.getValue(1);
        float lon = (float) input.getValue(2);
        int no2 = (int) input.getValue(3);
        int ozone = (int) input.getValue(4);
        float temp = (float) input.getValue(5);
        float co = (float) input.getValue(6);
        float particles = (float) input.getValue(7);
        long generated = (long) input.getValue(8);
        int region = getRegion(lat, lon);
        long timeframe = ((long) (((long) input.getValue(8))/AggregateFrequency)) * AggregateFrequency;
        //if sensors are not working, discard tuple
        if (!(no2>=999 || ozone>=999 || temp>=200 || co>=99.9 || particles >= 0.99 || region < 0 || generated <= 0)) {
            collector.emit(tuple(id, lat, lon, no2, ozone, temp, co, particles, region, generated, timeframe));
        }
    }

    private int getRegion(float lat, float lon){
        int region = -1;
        if (lat > 43.45 && lat < 43.47 && lon >= -3.83 && lon < -3.79){
            region = 0; //Santander
        } else if (lat >= 43.46 && lat < 43.48 && lon >= -3.79 && lon <= -3.77){
            region = 1; //Sardinero
        } else if ((lat >= 43.48 && lat < 43.49 && lon >= -3.81 && lon <= -3.77) ||
                (lat >= 43.47 && lat < 43.48 && lon >= -3.81 && lon < -3.79)){
            region = 2; //Valdenoja, Cueto y Universidades
        } else if (lat >= 43.47 && lat <= 43.50 && lon > -3.83 && lon < -3.81) {
            region = 3; //Monte
        } else if (lat >= 43.43 && lat <= 43.45 && lon >= -3.86 && lon <= -3.83) {
            region = 4; //Peñacastillo
        } else if (lat > 43.45 && lat <= 43.47 && lon > -3.87 && lon <= -3.83) {
            region = 5; //Albericia, Adarzo y Alisal
        }
        return region;
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "lat", "lon", "no2", "ozone", "temp", "co", "particles", "region", "generated", "timeframe"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}