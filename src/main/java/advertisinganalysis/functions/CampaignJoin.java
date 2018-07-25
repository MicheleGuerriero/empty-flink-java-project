package advertisinganalysis.functions;

import advertisinganalysis.datatypes.AdEvent;
import advertisinganalysis.datatypes.Ad;
import advertisinganalysis.datatypes.AdCampaign;
import advertisinganalysis.datatypes.CampaignAnalysis;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CampaignJoin extends RichCoFlatMapFunction<String, Ad, AdCampaign> {

	List<AdCampaign> adsToCampaigns;
	
	private static final Logger logger = LoggerFactory.getLogger(CampaignJoin.class);

	@Override
	public void open(Configuration conf) {
		adsToCampaigns = new ArrayList<AdCampaign>();
	}

	@Override
	public void flatMap1(String tuple, Collector<AdCampaign> out) throws Exception {
		logger.info("New ad to campaign entry: " + tuple);
		AdCampaign toAdd = new AdCampaign();

		String[] fields = tuple.split(",");

		toAdd.setCampaignId(fields[0]);
		toAdd.setAdId(fields[1]);
		toAdd.setEventTime(-1L);

		this.adsToCampaigns.add(toAdd);
	}

	@Override
	public void flatMap2(Ad tuple, Collector<AdCampaign> out) throws Exception {
		AdCampaign output = null;
		logger.info("New ad event: " + tuple);
		
		for (AdCampaign a : this.adsToCampaigns) {
			if (a.getAdId().equals(tuple.getAdId())) {
				output = new AdCampaign();
				output.setAdId(a.getAdId());
				output.setCampaignId(a.getCampaignId());
				output.setEventTime(tuple.getEventTime());
				output.setTupleId(tuple.getTupleId());
			}
		}

		if(output != null)
			out.collect(output);
	}

}
