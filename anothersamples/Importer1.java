import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;


public class Importer1{
	private static HTableInterface htable;

	public static void main(String[] args) throws IOException {
		String [] pages= {"/","/a.html","/b.html","/c.html"};
		Configuration hbaseConf=HBaseConfiguration.create();		
		htable = new HTable(hbaseConf, "access_log");
		ColumnDescriptor cd = new ColumnDescriptor();
		
			
		int totalRecords=100000;
		int maxID=totalRecords/100;
		Random rand=new Random();
		System.out.println("importing "+totalRecords+ " records ....");
		for (int i = 0; i < totalRecords; i++) {
			int userID=rand.nextInt(maxID)+1;
			byte [] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
			String randomPage=pages[rand.nextInt(pages.length)];
			Put put=new Put(rowkey);
			put.add(Bytes.toBytes("cf"),Bytes.toBytes("page"),Bytes.toBytes(randomPage));
			htable.put(put);
			
		}		
        System.out.println("done");
		
	}
	
	
}

