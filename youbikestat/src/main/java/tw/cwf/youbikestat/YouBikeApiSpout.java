package tw.cwf.youbikestat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class YouBikeApiSpout extends BaseRichSpout {
	SpoutOutputCollector out;
	
	public void nextTuple() {
		try {
			String json = Util.execGet("http://data.taipei/youbike");
			//System.err.println(json);
			out.emit(new Values(json));
		} catch(Exception e) {
			e.printStackTrace();
		}
		Utils.sleep(60000);
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		out = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}

}
