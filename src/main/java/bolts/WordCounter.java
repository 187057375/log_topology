package bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {

	Integer id;
	String name;
	Map<String, Integer> counters;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * we will show the word counters
	 */
	@Override
	public void cleanup() {
		try {
            BufferedWriter out = new BufferedWriter(new FileWriter("src/main/resources/out.txt"));
            System.out.println("-- Word Counter ["+name+"-"+id+"] --");
            for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                System.out.println(entry.getKey()+": "+entry.getValue());            
                out.write(entry.getKey()+": "+entry.getValue()+"\n");                
            }
            out.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
	}

	/**
	 * On create
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		/**
		 * If the word dosn't exist in the map we will create
		 * this, if not we will add 1 
		 */
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}
}