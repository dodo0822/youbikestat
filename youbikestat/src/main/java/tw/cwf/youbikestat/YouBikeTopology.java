package tw.cwf.youbikestat;

import java.util.Iterator;
import java.util.Map;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.json.JSONObject;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class YouBikeTopology {
	
	public static class DeserializeBolt extends BaseBasicBolt {

		public void execute(Tuple tuple, BasicOutputCollector out) {
			String json = tuple.getString(0);
			//System.err.println(json);
			try {
				JSONObject jobj = new JSONObject(json);
				JSONObject retVal = jobj.getJSONObject("retVal");
				Iterator<String> iter = retVal.keys();
				while(iter.hasNext()) {
					String key = iter.next();
					JSONObject station = retVal.getJSONObject(key);
					String id = station.getString("sno");
					String name = station.getString("sna");
					String total = station.getString("tot");
					String avail = station.getString("sbi");
					out.emit(new Values(id, name, avail, total));
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "name", "avail", "total"));
		}
		
	}
	
	/*public static class CalcBolt extends BaseBasicBolt {
		
		public void execute(Tuple tuple, BasicOutputCollector out) {
			String name = tuple.getStringByField("name");
			Integer avail = tuple.getIntegerByField("avail");
			Integer total = tuple.getIntegerByField("total");
			Float percentage = (Float.valueOf(avail)) / total * 100;
			out.emit(new Values(name, percentage));
			//System.out.println(name + " ==> " + String.valueOf(percentage));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("name", "availPercentage"));
		}
		
	}*/
	
	public static String host = "10.0.0.121";
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new YouBikeApiSpout(), 1);
		builder.setBolt("deserialize", new DeserializeBolt(), 1).shuffleGrouping("spout");
		//builder.setBolt("calc", new CalcBolt(), 4).shuffleGrouping("deserialize");
		
		SimpleHBaseMapper mapper1 = new SimpleHBaseMapper()
				.withRowKeyField("id")
				.withColumnFields(new Fields("avail", "total"))
				.withColumnFamily("data");
		
		HBaseBolt hbaseBolt1 = new HBaseBolt("YouBike", mapper1)
				.withConfigKey("hbase.conf");
		
		SimpleHBaseMapper mapper2 = new SimpleHBaseMapper()
				.withRowKeyField("id")
				.withColumnFields(new Fields("name"))
				.withColumnFamily("name");
		
		HBaseBolt hbaseBolt2 = new HBaseBolt("YouBike", mapper2)
				.withConfigKey("hbase.conf");
		
		builder.setBolt("hbase_data", hbaseBolt1, 4).shuffleGrouping("deserialize");
		builder.setBolt("hbase_name", hbaseBolt2, 4).shuffleGrouping("deserialize");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		conf.put(Config.NIMBUS_HOST, host);
		Map stormConf = Utils.readStormConfig();
		stormConf.put("nimbus.host", host);
		Map hbaseConf = new Config();
		hbaseConf.put("hbase.rootdir", "hdfs://localhost:9000/hbase");
		hbaseConf.put("hbase.zookeeper.quorum", "localhost");
		stormConf.put("hbase.conf", hbaseConf);
		Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
		String inputJar = YouBikeTopology.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
		NimbusClient nimbus = new NimbusClient(stormConf, host, 6627);
		conf.setNumWorkers(3);
		String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
		String jsonConf = new JSONObject(stormConf).toString();
		nimbus.getClient().submitTopology("testTopology", uploadedJarLocation, jsonConf, builder.createTopology());
		
	}
	
}
