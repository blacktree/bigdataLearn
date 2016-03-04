package hbase.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class MapperTest {

	private static final int DEFAULT_CACHE_SIZE = 0;
	private static final int DEFAULT_BATCH_SIZE = 0;

	public static Configuration configuration;  
	
	static {  
		configuration = HBaseConfiguration.create();  
		configuration.set("hbase.zookeeper.property.clientPort", "2181");  
		configuration.set("hbase.zookeeper.quorum", "hbase-test-253,hbase-test-254,hbase-test-255");  
		configuration.set("hbase.master", "hbase-test-253:600000");  
	}  

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		mappReduce();
	}

	
	
	public static boolean mappReduce() {

		String startKey = "";
		String stopKey ="";
		Scan scan =   new Scan(Bytes.toBytes(startKey), Bytes.toBytes(stopKey));
		// 设置 caching and batch 提高查询性能及防止暴力OOM及rpc timeout
		scan.setCaching(DEFAULT_CACHE_SIZE );
		scan.setBatch(DEFAULT_BATCH_SIZE);
		Job job;
		try {
			configuration.set("param", String.valueOf("param_value"));
			job = new Job(configuration,"ReadDataFromHBase");
			job.setOutputFormatClass(NullOutputFormat.class);
			TableMapReduceUtil.initTableMapperJob("tableName", scan, DealCounterMapper.class,  
					ImmutableBytesWritable.class, Result.class, job); 
			//直接入redis不需要reduce    
			job.setNumReduceTasks(0);
			return job.waitForCompletion(true);
			//job.waitForCompletion(true)? 0 : 1; 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		return false;
	}
}
