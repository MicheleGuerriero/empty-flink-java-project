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

import java.util.*;

public class AdEventParser extends RichMapFunction<String, AdEvent> {
	
	private static final Logger logger = LoggerFactory.getLogger(AdEventParser.class);

	@Override
	public void open(Configuration conf) {

	}

	@Override
	public AdEvent map(String tuple) throws Exception {
		logger.info("Parsing ad event.");
		JSONObject obj = new JSONObject(tuple);
		return new AdEvent(obj.getString("user_id"), obj.getString("page_id"), obj.getString("ad_id"), obj.getString("ad_type"), obj.getString("event_type"), Long.parseLong(obj.getString("event_time")), obj.getString("ip_address"));
	}

}
