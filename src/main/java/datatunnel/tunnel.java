package datatunnel;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class tunnel implements Callable<Integer>{

	final static String initialPath = "C:\\Users\\DJ\\Desktop\\JAVA";
	final static int threadCount = 5;
	final static int batchThreshold = 100;
	
	public int myNumber ;
	public int queryCnt ;
	public int projectsMetrics = 0;
	//public BulkRequestBuilder bulkRequest = null;
	private String[] params = new String[5];
	private TransportClient client;
	
	public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {
		
		List<Future<Integer>> results = new ArrayList<Future<Integer>>();
		
		ExecutorService es = Executors.newFixedThreadPool(threadCount);
		for(int i=0;i<threadCount;i++)
		{
			results.add( es.submit(new tunnel(i)) );
		}
		
		int sum = 0;
		for( Future<Integer> each : results)
		{
			System.out.print(each.get()+" ");
			sum += each.get();
		}
		System.out.println("\n"+sum);
		es.shutdown();
	}
	
	public Integer call() throws Exception {
		
		File file = new File(initialPath);
		init();
		indexAllData(file, 0, 0);
		close();
		return projectsMetrics;
	}
	
	public tunnel(int num)
	{
		this.myNumber = num;
	}
	
	private void init() throws UnknownHostException
	{
		queryCnt = 0;
		
		Settings settings = Settings.builder()
				.put("client.transport.sniff", true) 
				.put("cluster.name", "dj_cluster")
				.build();  
		client = new TransportClient.Builder().settings(settings).build();
//		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.168"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.142"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.221"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.222"), 9300))
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.105"), 9300))
//				;  
//		
		
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
//		bulkRequest = client.prepareBulk();
		
		//检查用
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
	
	public void indexAllData(File file,int depth,int order)
	{
		if(file.getName().equals("details.txt"))
			return;
		
		if (depth == 1)
		{
			if( order%threadCount != myNumber )
			{
				return;
			}
			
			projectsMetrics++;
			if(projectsMetrics%50 == 0)
			{
				System.out.println( myNumber + ": I am processing "+ projectsMetrics + " project! Oh yeah, fighting");
				System.out.println( "queryCnt " + queryCnt);
			}

			File[] twofiles = file.listFiles();
			if(twofiles[0].isDirectory())
				updateParams(twofiles[1]);
			else
				updateParams(twofiles[0]);
		}
		
		if (file.isDirectory())
		{
			File[] files = file.listFiles();
			
			for(int i=0; i<files.length; i++)
			{
				indexAllData(files[i], depth+1, i);
			}
		}
		else
		{
			params[3] = file.getName().trim();
			insertDoc(file);
		}
	}

	private void insertDoc(File file) {
		
//		BulkResponse resp = null;
		
		IndexResponse resp = null;
		
		String lines = "";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String tmp = null;
			while( (tmp=br.readLine())!= null )
				lines = lines + "\n" + tmp;
			br.close();
		}
		catch(Exception e)
		{
			System.out.println(file.getAbsolutePath() + " " + file.getName());
			e.printStackTrace();
		}
		
		try
		{
			XContentBuilder builder = jsonBuilder()  
				    .startObject()  
				        .field("projectName", params[0])  
				        .field("userName", params[1])  
				        .field("url", params[2])
				        .field("fileName", params[3])
				        .field("content", lines)
				    .endObject();  
			String json = builder.string(); 
			
			resp = client.prepareIndex("javacode", "github").setSource(json).execute().actionGet();
			
			queryCnt++;			
			
//			bulkRequest = client.prepareBulk();
//			bulkRequest.add( client.prepareIndex("javacode", "github").setSource(json) );
//			queryCnt++;
//			
//			if( queryCnt >= batchThreshold )
//			{
//				queryCnt = 0;
//				resp = bulkRequest.execute().actionGet();
//				bulkRequest = client.prepareBulk();
//			}
		}
		catch(Exception e)
		{
			System.out.println("目前在处理" + projectsMetrics);
			System.out.println("queryCnt: " + queryCnt);
			System.out.println(resp.toString());
			e.printStackTrace();
		}
	}
	
	private void updateParams(File file)
	{
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = null;
			int seq = 0;
			while( (line=br.readLine())!= null )
			{
				params[seq] = line.split(":", 2)[1].trim();
				seq++;
			}
			br.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	


}