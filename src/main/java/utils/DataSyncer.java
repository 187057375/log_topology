
package utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.Config;
import redis.clients.jedis.Jedis;

public class DataSyncer implements Runnable {

    private final long delay;

    private final boolean end;

    private volatile boolean run = true;

    private Map conf;
    
    public DataSyncer(Map conf) {
        this(conf,1000);
    }

    public DataSyncer(Map conf, long delay) {
        this(conf, 1000, false);
    }

    public DataSyncer(Map conf, long delay, boolean end) {

        this.conf = conf;
        this.delay = delay;
        this.end = end;

    }

    public static DataSyncer create(Map conf, long delay, boolean end) {
    	DataSyncer DataSyncer = new DataSyncer(conf, delay,end);
        Thread thread = new Thread(DataSyncer);
        thread.setDaemon(true);
        thread.start();
        return DataSyncer;
    }


    public static DataSyncer create(Map conf, long delay) {
        return create(conf, delay, false);
    }

    public static DataSyncer create(Config conf) {
        return create(conf, 1000, false);
    }

    public Map getConf() {
        return conf;
    }

    public long getDelay() {
        return delay;
    }

    public void run() {
			String host = conf.get("redis-host").toString();
			int port = Integer.valueOf(conf.get("redis-port").toString());
			Jedis jedis = new Jedis(host, port);
			jedis.select(3);
			
			// mysql connection
			ConnectDB cd=new ConnectDB();
			Connection conn = cd.ConnectMysql();
			
			try{
				Statement stmt = conn.createStatement();
		    }catch(Exception e){
		    	e.printStackTrace();
		    }
			
			while(run){
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				String insql="insert into test_data(id,pv) values('1',100)";
		
		        try {
		            PreparedStatement ps = conn.prepareStatement(insql);
		            int result=ps.executeUpdate();//line num or 0
	//	            if(result>0)
	//	                return true;
		        } catch (SQLException ex) {
		        	ex.printStackTrace();
		        }
	
			}

    }

    public void stop() {
        this.run = false;
    }
}



//