package advertisinganalysis.functions;

import advertisinganalysis.datatypes.AdEvent;
import advertisinganalysis.datatypes.Ad;
import advertisinganalysis.datatypes.AdCampaign;
import advertisinganalysis.datatypes.CampaignAnalysis;
import akka.stream.scaladsl.BroadcastHub.Open;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CampaignProcessor extends RichWindowFunction<AdCampaign, CampaignAnalysis, Tuple, TimeWindow> {

	protected Socket socket;
	
	@Override
	public void open(Configuration parameters) throws InterruptedException {
		try {
			this.socket = new Socket(InetAddress.getByName("localhost"), 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<AdCampaign> windowContentIterator,
			Collector<CampaignAnalysis> out) throws Exception {

		List<AdCampaign> windowContent = new ArrayList<AdCampaign>();
		for (AdCampaign x : windowContentIterator) {
			windowContent.add(x);
		}

		int count = 0;

		for (AdCampaign ad : windowContent) {
			count++;
		}

		CampaignAnalysis output = new CampaignAnalysis();
		output.setCampaignId(key.getField(0));
		output.setViewAdEventCount(count);
		output.setEventTime(window.getEnd());

		PrintStream socketWriter = new PrintStream(socket.getOutputStream());
		
		int max = -1;
		for(AdCampaign t: windowContent) {
			if(Integer.parseInt(t.getTupleId().substring(1)) > max) {
				max = Integer.parseInt(t.getTupleId().substring(1));
			}
		}

		socketWriter.println("t" + max+ "_end");	
		out.collect(output);
	}

}
