package topo;


import spouts.TailFileSpout;
import spouts.WordGenerator;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		
		//builder.setSpout("reader",new WordReader());
		
		//builder.setBolt("normalizer", new WordNormalizer())
		//	.shuffleGrouping("reader");
		TailFileSpout Tailspout = new TailFileSpout("src/main/resources/test.log");
		
		
		//builder.setSpout("generator",new WordGenerator(),1);
		builder.setSpout("generator",Tailspout,1);
		
		builder.setBolt("normalizer", new WordNormalizer(),1)
			.shuffleGrouping("generator");
		
		builder.setBolt("counter", new WordCounter(),1)
			.fieldsGrouping("normalizer", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.setMaxTaskParallelism(3);
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 3);
		conf.setDebug(true);
//		conf.setNumWorkers(10);
		
        //Topology run
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("storm-wordcount", conf, builder.createTopology());
//		Thread.sleep(600000);
//		cluster.shutdown();
		StormSubmitter.submitTopology("word_count", conf,builder.createTopology());
	}
}
