package spouts;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import logparser.PvLogParser;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;



public class RedisSpout extends BaseRichSpout{

	private static final long serialVersionUID = 4071287265800284501L;
	Jedis jedis;
	String host; 
	int port;
	SpoutOutputCollector collector;
			
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map stormConf, TopologyContext context,
			SpoutOutputCollector collector) {
		host = stormConf.get("redis-host").toString();
		port = Integer.valueOf(stormConf.get("redis-port").toString());
		this.collector = collector;
		reconnect();
	}
	
	private void reconnect() {
		jedis = new Jedis(host, port);
		jedis.select(2);
	}

	@Override
	public void nextTuple() {
		//String content = jedis.rpop("navigation");
		String content = jedis.rpop("raw");
		if(content==null || "nil".equals(content)) {
			try { Thread.sleep(10); } catch (InterruptedException e) {}
		} else {
	        JSONObject obj=(JSONObject)JSONValue.parse(content);
	        String message = obj.get("message").toString();
//	        String product = obj.get("product").toString();
//	        String type = obj.get("type").toString();
//	        HashMap<String, String> map = new HashMap<String, String>();
//	        map.put("product", product);
//	        NavigationEntry entry = new NavigationEntry(user, type, map);
	        String query = "";
	        try {
				Map<String,String> map = PvLogParser.parse(message);
				query = map.get("query"); 
			} catch (ParseException e) {
				e.printStackTrace();
			}
			collector.emit(new Values(query));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
	
	
}
