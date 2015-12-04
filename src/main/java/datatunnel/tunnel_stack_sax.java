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
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class tunnel_stack_sax implements Callable<Integer>{

//	final static String initialPath = "G:\\192.168.3.168";
//	final static String initialPath = "C:\\Users\\DJ\\Desktop\\act\\Crawler\\test demo\\posts_page1.xml";
//	final static String initialPath = "/usr/dj/Crawl_data/StackOverFlow";
	final static String initialPath = "G:\\seperate";
	final static int threadCount = 5;
	final static int batchThreshold = 100;
	
	public int myNumber ;
	public int queryCnt ;

	private TransportClient client;
	
	public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {
		long StartTime = System.currentTimeMillis();
		
		List<Future<Integer>> results = new ArrayList<Future<Integer>>();
		
		ExecutorService es = Executors.newFixedThreadPool(threadCount);
		for(int i=0;i<threadCount;i++)
		{
			results.add( es.submit(new tunnel_stack_sax(i)) );
		}
		
		int sum = 0;
		for( Future<Integer> each : results)
		{
			System.out.print(each.get()+" ");
			sum += each.get();
		}
		System.out.println("\n"+sum);
		es.shutdown();
		System.out.println(System.currentTimeMillis()-StartTime);
	}
	
	public Integer call() throws Exception {
		
		File file = new File(initialPath);
		init();
		indexAllData(file,0);
		close();
		return this.queryCnt;
	}
	
	public tunnel_stack_sax(int num)
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
//		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.168"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.142"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.222"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.105"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.223"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.224"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.234"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.237"), 9300))
//				;  
		
		
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

		
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
	

	
	public void indexAllData(File file,int order){
		
		if (file.isDirectory()){
			File[] files = file.listFiles();
			
			for(int i=0; i<files.length; i++){
				indexAllData(files[i],i);
			}
		}
		else{
			try {
				if (order%threadCount != myNumber)
					return;
				parserXML(file);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println(file.getPath());
			}
		}
	}
	
	private void parserXML(File file){
		SAXParserFactory saxfac = SAXParserFactory.newInstance();
		
		try {
            SAXParser saxparser = saxfac.newSAXParser();
            InputStream is = new FileInputStream(file);
            saxparser.parse(is, new MySAXHandler(file.getName()));
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	
	class MySAXHandler extends DefaultHandler {
	    private String currentQname;
	    private XContentBuilder builder;
	    private IndexResponse resp = null;
	    private String TypeName;
	    private List<String> Tags;
	    private String Body;
	    
	    public MySAXHandler(String filename){
	    	if (filename.contains("user")){
	    		this.TypeName = "stack_user";
	    	}
	    	else if(filename.contains("posts")){
	    		this.TypeName = "stack_post";
	    	}
	    	else if(filename.contains("comments")){
	    		this.TypeName = "stack_comment";
	    	}
	    }

	    public void startDocument() throws SAXException {
//	         System.out.println("Start XML Document!");
	    }

	    public void endDocument() throws SAXException {
//	         System.out.println("Finish XML Document!");
	    }

	    // find 'item' tag
	    public void startElement(String uri, String localName, String qName,
	            Attributes attributes) throws SAXException {
	        if (qName.equals("item")) {
	        	try {
					this.builder = jsonBuilder()  
						    .startObject();
					if(queryCnt%10000 == 0)
					{
						System.out.println( myNumber + ": I am inserting "+ queryCnt + " query! Oh yeah, fighting");
					}
					Tags = new ArrayList<String>();
					Body = "";
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        this.currentQname = qName;	        
	        
	    }

	    public void endElement(String uri, String localName, String qName)
	            throws SAXException {
	        if (qName.equals("item")){
	        	try {
	        		this.builder.field("Tags",Tags);
	        		this.builder.field("Body",this.Body);
					String json = this.builder.string();
					resp = client.prepareIndex("test_2", "user").setSource(json).execute().actionGet();
					queryCnt ++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.out.println(resp.toString());
					e.printStackTrace();
				}
	        }
	    }

	    public void characters(char[] ch, int start, int length)
	            throws SAXException {
	    	String content = new String(ch, start, length);
	        if(!(this.currentQname.equals("item") || this.currentQname.equals("items"))){
	        	try {
	        		if (this.currentQname.equals("value")){
	        			this.Tags.add(content);
	        		}
	        		else if(this.currentQname.equals("Body")){
	        			this.Body += content;
	        		}
	        		else{
	        			this.builder.field(this.currentQname,content);
	        		}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        
	    }
	}
	
}



