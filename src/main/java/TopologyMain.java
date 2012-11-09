import spouts.WordGenerator;
import spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
        
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		
		//builder.setSpout("reader",new WordReader());
		
		//builder.setBolt("normalizer", new WordNormalizer())
		//	.shuffleGrouping("reader");
		
		builder.setSpout("generator",new WordGenerator());
		
		builder.setBolt("normalizer", new WordNormalizer())
			.shuffleGrouping("generator");
		
		builder.setBolt("counter", new WordCounter())
			.fieldsGrouping("normalizer", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.setMaxTaskParallelism(3);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 3);
		conf.setDebug(true);
		
        //Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-wordcount", conf, builder.createTopology());
		Thread.sleep(30000);
		cluster.shutdown();
	}
}
