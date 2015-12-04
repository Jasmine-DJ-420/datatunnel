package datatunnel;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class testES {

	private TransportClient client;
	
	public static void main(String[] args) throws IOException {
		
		testES t = new testES();
		t.init();
		t.insertDoc();
		t.search();
		t.close();
		
	}
	
	private void search()
	{
//		QueryBuilder query = matchQuery("url", "https://github.com/47deg/firebrand");
		
		
		QueryBuilder query = wildcardQuery("url", "*firebrand*");
		
		SearchResponse response = client.prepareSearch("javacode")
				.setTypes("github")
				       .setQuery(query)
				       .setSize(60)
				       .execute()
				       .actionGet();
		SearchHits shs = response.getHits();
		
		System.out.println(shs.totalHits());
		
		for(SearchHit hit : shs)
		{
			System.out.println("·ÖÊý(score):"+hit.getScore()+", url:"+
			hit.getSource().get("url"));
		}
	}
	
	private void insertDoc() throws IOException
	{
		
		IndexResponse res = client.prepareIndex("test","type1")
				.setSource("{\"desc\": " + System.currentTimeMillis() +", \"name\":\"xzl\", \"age\": \"11\"}")
				.execute().actionGet();
		System.out.println(res);
	}
	
	private void init() throws UnknownHostException
	{
		Settings settings = Settings.builder()
				.put("client.transport.sniff", true) 
				.put("cluster.name", "sdp")
				.build();  
		client = new TransportClient.Builder().settings(settings).build();
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.168"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.142"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.221"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.222"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.105"), 9300))
				;  
	}
	
	private void close()
	{
		client.close();
	}

}
