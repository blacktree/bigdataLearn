package hbase.mapper;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
 

public class DealCounterMapper extends TableMapper<ImmutableBytesWritable,Put> {

	private String param;
 

	@Override
	protected void setup(Context ctx)throws IOException, InterruptedException {
		//		dayTable = new HTable(ctx.getConfiguration(),ctx.getConfiguration().get("dayTable"));
		//		dayTable.setAutoFlush(false);
		param=ctx.getConfiguration().get("param");
	}

	@Override 
	protected void cleanup(Context cxt){
		//        try {
		//        	dayTable.put(list);
		//        	dayTable.flushCommits();
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
	}

	protected void map(ImmutableBytesWritable key, Result rs,  
			Context ctx) throws IOException, InterruptedException { 

		KeyValue[] keyValues = rs.raw();
		if (keyValues != null && keyValues.length > 0) {
			for (KeyValue keyValue : keyValues) { // 遍历
              //doSth
			}
		}

	}

}