package bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;


import utils.DataSyncer;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class WordCounter extends BaseBasicBolt {

	private DataSyncer syncer;
	private long interval;
	private Integer id;
	private String name;
	private Map<String, Integer> counters;
	private BufferedWriter out = null;
	private Integer pv = 0;
	private Jedis jedis;
	private String host; 
	private int port;
	private int db;


	/**
	 * At the end of the spout (when the cluster is shutdown
	 * we will show the word counters
	 */
	@Override
	public void cleanup() {
		/*
		try {
            //BufferedWriter out = new BufferedWriter(new FileWriter("src/main/resources/out.txt"));
            System.out.println("-- Word Counter ["+name+"-"+id+"] --");
            for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                System.out.println(entry.getKey()+": "+entry.getValue());            
                out.write(entry.getKey()+": "+entry.getValue()+"\n");                
            }
            out.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
        */
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		syncer.stop();
		
	}

	//connect redis
	private void reconnect() {
		jedis = new Jedis(host, port);
		jedis.select(db);
	}
	
	/**
	 * On create
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.pv = 0;
		
		//out to redis
		host = stormConf.get("redis-host").toString();
		port = Integer.valueOf(stormConf.get("redis-port").toString());
		db = Integer.valueOf(stormConf.get("stat-db").toString());
		reconnect();

        //out to file
		try {
			this.out = new BufferedWriter(new FileWriter("/Users/work/work/log_topology/out.txt"),1);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println("Error open file out.txt !"); 
			e1.printStackTrace();
		}
		
		
		// redis to mysql
		interval = Long.valueOf(stormConf.get("interval").toString());
		syncer = DataSyncer.create(stormConf,interval);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		
			try {
				out.write(str + "\n");
				out.flush();
	        } catch (IOException e) {
	        	e.printStackTrace();
	        }

		}
		
	
}


