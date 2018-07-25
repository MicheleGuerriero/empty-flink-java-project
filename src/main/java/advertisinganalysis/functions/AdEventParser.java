package advertisinganalysis.functions;

import advertisinganalysis.datatypes.AdEvent;
import advertisinganalysis.datatypes.Ad;
import advertisinganalysis.datatypes.AdCampaign;
import advertisinganalysis.datatypes.CampaignAnalysis;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

public class AdEventParser extends RichMapFunction<String, AdEvent> {
	
	private static final Logger logger = LoggerFactory.getLogger(AdEventParser.class);

	protected Socket socket;
	
	private int counter = 1;
	
	@Override
	public void open(Configuration conf) {
		try {
			this.socket = new Socket(InetAddress.getByName("localhost"), 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public AdEvent map(String tuple) throws Exception {
		logger.info("Parsing ad event.");
		PrintStream socketWriter = new PrintStream(socket.getOutputStream());
		socketWriter.println("t" + this.getRuntimeContext().getIndexOfThisSubtask() + counter + "_start");
		JSONObject obj = new JSONObject(tuple);
		AdEvent toReturn = new AdEvent(obj.getString("user_id"), obj.getString("ad_id"), obj.getString("page_id"), obj.getString("ad_type"), obj.getString("event_type"), Long.parseLong(obj.getString("event_time")), obj.getString("ip_address"));
		toReturn.setTupleId("t" + this.getRuntimeContext().getIndexOfThisSubtask() + counter);
		counter = counter + 1;
		return toReturn;
	}

}
