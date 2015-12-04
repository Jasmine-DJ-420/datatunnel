package datatunnel;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.w3c.dom.*;



public class tunnel_stack implements Callable<Integer>{

//	final static String initialPath = "G:\\192.168.3.168";
//	final static String initialPath = "C:\\Users\\DJ\\Desktop\\act\\Crawler\\test demo\\posts_page1.xml";
	final static String initialPath = "/usr/dj/Crawl_data/StackOverFlow_seperate/";
//	final static String initialPath = "G:\\seperate";
	final static int threadCount = 5;
	final static int batchThreshold = 100;
	
	public int myNumber ;
	public int queryCnt = 0;
	public int FileNum = 0;
	private String TypeName;

	private TransportClient client;
	
	public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {
		
		long startTime = System.currentTimeMillis();
		
		List<Future<Integer>> results = new ArrayList<Future<Integer>>();
		
		ExecutorService es = Executors.newFixedThreadPool(threadCount);
		for(int i=0;i<threadCount;i++)
		{
			results.add( es.submit(new tunnel_stack(i)) );
		}
		
		int sum = 0;
		for( Future<Integer> each : results)
		{
			System.out.print(each.get()+" ");
			sum += each.get();
		}
		System.out.println("\n"+sum);
		es.shutdown();
		System.out.println(System.currentTimeMillis()-startTime);
	}
	
	public Integer call() throws Exception {
		
		File file = new File(initialPath);
		init();
		indexAllData(file,0);
		close();
		return this.queryCnt;
	}
	
	public tunnel_stack(int num)
	{
		this.myNumber = num;
	}
	
	private void init() throws UnknownHostException
	{
		queryCnt = 0;
		
		Settings settings = Settings.builder()
				.put("client.transport.sniff", true) 
				.put("cluster.name", "sdp")
				.build();  
		client = new TransportClient.Builder().settings(settings).build();
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.168"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.142"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.222"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.105"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.223"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.224"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.234"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.237"), 9300))
				;  
		
		
//		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

		
		//ºÏ≤È”√
		IndexResponse res = client.prepareIndex("test","type1")
							.setSource("{\"desc\": " + System.currentTimeMillis() +", \"name\":\"xzl\", \"age\": \"11\"}")
							.execute().actionGet();
		
		if(!res.isCreated()) 
		{
			System.out.println("failed "+ res.toString());
			System.exit(0);
		}
	}
	
	private void close()
	{
		client.close();
	}
	

	
	public void indexAllData(File file,int order) throws Exception{
		
		if (file.isDirectory()){
			File[] files = file.listFiles();
			
			for(int i=0; i<files.length; i++){
				indexAllData(files[i],i);
			}
		}
		else{			
			if (order%threadCount != myNumber)
				return;
			parserXML(file);
			
		}
	}
	
	private void parserXML(File file) throws Exception{
		// filename
		String filename = file.getName();
		if (filename.contains("user")){
    		TypeName = "stack_user";
    	}
    	else if(filename.contains("posts")){
    		TypeName = "stack_post";
    	}
    	else if(filename.contains("comments")){
    		TypeName = "stack_comment";
    	}
		
		FileNum++;
		if(FileNum%10 == 0)
		{
			System.out.println( myNumber + ": I am inserting No."+ FileNum + " file! Oh yeah, fighting");
			System.out.println("filename is " + file.getName());
		}
		
		DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();

		Document doc = dBuilder.parse(file);
		
		Element root = doc.getDocumentElement();
		
		NodeList nList = root.getElementsByTagName("item");
		
		for(int temp = 0; temp < nList.getLength(); temp++){
			Node nNode = nList.item(temp);
			if (nNode.hasChildNodes()){
				indexItem(nNode.getChildNodes(),TypeName);
			}
		}
		
	}
	
	private void indexItem (NodeList nList, String TypeName) throws IOException{
		XContentBuilder builder;
		builder = jsonBuilder().startObject();
		boolean flag_search = true;
		
		List<String> Tags = new ArrayList<String>();
		try {
		for(int temp = 0; temp < nList.getLength(); temp++){
				Node nNode = nList.item(temp);
				if (nNode.getNodeName().equals("Tags")){
					if (nNode.hasChildNodes()){
						NodeList tagsNode = nNode.getChildNodes();
						for (int ttemp=0;ttemp<tagsNode.getLength();ttemp++){
							Tags.add(tagsNode.item(ttemp).getTextContent());
						}
						builder.field("Tags",Tags);
					}
				}else{
					if (nNode.getNodeName().equals("Id")){
						SearchResponse response = client.prepareSearch("stackoverflow")
								.setTypes(TypeName)
								       .setQuery(QueryBuilders.termQuery("Id", nNode.getTextContent()))
								       .setSize(60)
								       .execute()
								       .actionGet();
						SearchHits shs = response.getHits();
						if(shs.totalHits()!=0){
							flag_search = false;
							break;
						}
					}
					builder.field(nNode.getNodeName(),nNode.getTextContent());				
				}
		}
		if (flag_search){
			String json = builder.string();
			client.prepareIndex("stackoverflow", TypeName).setSource(json).execute().actionGet();
			queryCnt ++;
		}
		} catch (DOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

		
}



