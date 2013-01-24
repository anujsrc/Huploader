package com.hbase.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.util.XHBaseConstant;


public class HBaseUtil {

	public static final Log log = LogFactory.getLog(HBaseUtil.class);
	private Configuration conf = null;
	private HBaseAdmin admin = null;
	private HTable table = null;
	private int cacheSize = -1;
	private boolean blockCached = true;
	private HashMap<String,HRegionInfo> regions = null;
	
	public HBaseUtil(String confPath){			
		try{
			
			this.conf = HBaseConfiguration.create();
			this.conf = this.loadCustomizedConf(confPath);
								
			System.out.println("cache size : "+cacheSize);
			System.out.println("retries number: "+this.conf.get("hbase.client.retries.number"));
			System.out.println("pause: "+this.conf.get("hbase.client.pause"));			
			
			this.admin = new HBaseAdmin(this.conf);
			regions = new HashMap<String,HRegionInfo>();
			
		}catch(Exception e){
			e.printStackTrace();
			log.info(e.fillInStackTrace());
		}		
	}

	private Configuration loadCustomizedConf(String confPath){			
		try{
			if(confPath != null){
				File directory = new File(confPath);
				if(directory.exists()){
					if(directory.isDirectory()){
						File[] files = directory.listFiles();
						for(int i=0;i<files.length;i++){
							System.out.println("resource is added "+files[i].getAbsolutePath());
							this.conf.addResource(new Path(files[i].getAbsolutePath()));
						}					
					}else{
						System.out.println("resource is added "+directory.getAbsolutePath());
						// new Path is required to fix the bug which cannot load the resource
						this.conf.addResource(new Path(directory.getAbsolutePath())); 
					}				
				}				
			}										
		}catch(Exception e){
			e.printStackTrace();
		}
		return conf;
	}
	
	public Configuration getHBaseConfig() {
		return conf;
	}
	
	public void setScanConfig(int cacheSize,boolean blockCache){
		this.cacheSize = cacheSize;
		this.blockCached = blockCache;
		System.out.println("in setScanConfig set the scan cache : "+ cacheSize);
	}

	public HashMap<String,HRegionInfo> getRegions(String tablename){
		try{
			List<HRegionInfo> list = this.admin.getTableRegions(Bytes.toBytes(tablename));
			for(int i=0;i<list.size();i++){
				this.regions.put(list.get(i).getRegionNameAsString(), list.get(i));				
			}				
			
			
		}catch(Exception e){
			e.printStackTrace();
		}	
		return this.regions;
	}
	
	public HTable createTable(String tableName, String[] metrics,int[] max_version) throws IOException {				
		System.out.println("create table for "+tableName);
		try{
			if (admin.tableExists(tableName)) {
				System.out.println(admin.listTables());
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}			
			HTableDescriptor td = this.createTableDescription(tableName, metrics,max_version);
			System.out.println(tableName + ": <=>table descirption : "+td.toString());
			this.admin.createTable(td);	
			//this.admin.createTable(td, ("0-").getBytes(), ("2-"+Long.MAX_VALUE).getBytes(),80);
			
		}catch(Exception e){
			e.printStackTrace();
			//log.info(e.fillInStackTrace());			
		}			
		return new HTable(conf, tableName);
	}
	/**
	 * 
	 * @param table
	 * @return
	 */
	public boolean createTable(JSONObject table){
		
		try{
			String table_name = "";
			if(!table.containsKey(XHBaseConstant.TABLE_DESC_NAME))
				return false;
			
			table_name = (String) table.get(XHBaseConstant.TABLE_DESC_NAME);
			if(table_name.isEmpty())
				return false;																		

			if(admin.tableExists(table_name)){
				admin.disableTable(table_name);
				admin.deleteTable(table_name);
			}
			
			HTableDescriptor td = this.createTableDescription(table);
			this.admin.createTable(td);
						
			return true;
			
		}catch(Exception e){
			e.printStackTrace();
		}	
		
		return false;
	}
	
	
	public HTable getTableHandler(String tableName){
		try{
			table = new HTable(conf, tableName);
			table.setAutoFlush(false);	
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return table;
	}
	
	public HTable getHTable(){
		return this.table;
	}
	
	
	public void insertRow(String rowKey,String[] families, String[] qualifiers, long ts,String[] values) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		
		Put put = new Put(rowKey.getBytes());
		for(int i=0;i<families.length;i++){
			if(ts > 0){
				put.add(families[i].getBytes(), qualifiers[i].getBytes(), ts, values[i].getBytes());
			}else{
				put.add(families[i].getBytes(), qualifiers[i].getBytes(),values[i].getBytes());
			}			
		}
		table.put(put);
	}
	
	public Put constructRow(String rowKey,String[] families, String[] qualifiers, long ts,String[] values) throws Exception{
		if(table == null)
			throw new Exception("!!!!!!!!!!! No table handler");
		
		Put put = new Put(rowKey.getBytes());
		put.setWriteToWAL(false);
		for(int i=0;i<families.length;i++){
			if(ts >= 0){
				put.add(families[i].getBytes(), qualifiers[i].getBytes(), ts, values[i].getBytes());
			}else{
				put.add(families[i].getBytes(), qualifiers[i].getBytes(),values[i].getBytes());
			}			
		}		
		
		return put;
		
	}
	
	public Put constructRow(String rowKey,String family, String[] qualifiers, long ts,String[] values) throws Exception{
		if(table == null)
			throw new Exception("!!!!!!!!!!! No table handler");
		
		Put put = new Put(rowKey.getBytes());
		put.setWriteToWAL(false);
		for(int i=0;i<qualifiers.length;i++){
			if(ts >= 0){
				put.add(family.getBytes(), qualifiers[i].getBytes(), ts, values[i].getBytes());
			}else{
				put.add(family.getBytes(), qualifiers[i].getBytes(),values[i].getBytes());
			}			
		}		
		
		return put;
		
	}	
	/**
	 * Only for debug return the execution time
	 * @param putList
	 * @return
	 * @throws Exception
	 */
	public long flushBufferedRow(ArrayList<Put> putList)throws Exception{
		long start = System.currentTimeMillis();
		try{			
			table.setAutoFlush(false);
			//table.setWriteBufferSize(1024*1024*12);
			table.put(putList);	
			table.setAutoFlush(true);			
		}catch(Exception e){
			e.printStackTrace();
		}
		return (System.currentTimeMillis()-start);		
	}
		
	public void closeTableHandler(){
		try{
			if (table != null) 
				table.close();		
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	// @deprecated because of new HTable();
//	public HTable updateTable(String tableName,String[] metrics,int[] max_version)throws IOException{
//		//log.info("entry: "+tableName + ":"+metrics);
//		try{
//			
//			HTableDescriptor td = this.createTableDescription(tableName, metrics,max_version);
//			this.admin.disableTable(tableName);
//			this.admin.modifyTable(tableName.getBytes(), td);
//			this.admin.enableTable(tableName);	
//			
//		}catch(Exception e){
//			log.info(e.fillInStackTrace());
//			e.printStackTrace();
//		}
//		//log.info("exit");
//		return new HTable(tableName);
//
//	}
	
	public void deleteTable(String tableName)throws IOException{
		//log.info("entry: "+tableName);
		try{			
			if(this.admin.tableExists(tableName)){
				this.admin.disableTable(tableName);
				this.admin.deleteTable(tableName);
			}			
		}catch(Exception e){
			log.equals(e.fillInStackTrace());
			e.printStackTrace();
		}
		//log.info("exit");
	}
		
	//create 'mytable', {NAME=>'colfam:', COMPRESSION=>'lzo'}
	private synchronized HTableDescriptor createTableDescription(String tableName,String[] metrics,int[] max_version){
		//log.info("entry: "+tableName + ":"+metrics);
		HTableDescriptor td = new HTableDescriptor(tableName);
		try{
			for (int i = 0; i < metrics.length; i++) {				
				String colName = metrics[i];				
				if (colName==null || colName.length() == 0) {
					log.info("Invalid table schema content, contains empty name column.");
					throw new Exception("Invalid table schema content, contains empty name column.");
				}
				HColumnDescriptor hcd = new HColumnDescriptor(colName);
				hcd.setMaxVersions(max_version[i]);
				//hcd.setBloomFilterType(BloomType.ROWCOL);
				
				// compress it and require to install LZO
				//hcd.setCompressionType(Compression.Algorithm.GZ);
				td.addFamily(hcd);
			}
			//td.setMaxFileSize(1073741824);
			
		}catch(Exception e){
			//log.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
		//log.info("exit");
		return td;				
	}
	
	
	//create table based on the jsonobject 
	private HTableDescriptor createTableDescription(JSONObject table){

		if(table == null)
			return null;
		
		HTableDescriptor td = null;
		try{
			
			String table_name = (String)table.get(XHBaseConstant.TABLE_DESC_NAME);
			td = new HTableDescriptor(table_name);
			JSONArray families = (JSONArray)table.get(XHBaseConstant.TABLE_DESC_FAMILIES);
			
			for(int i=0;i<families.size();i++){
				JSONObject object = (JSONObject)families.get(i);
				HColumnDescriptor one_family = getColumnDescriptor(object);
				td.addFamily(one_family);
			}			
			if(table.containsKey(XHBaseConstant.TABLE_DESC_MEM_STORE_SIZE)){
				String mem_store_size = (String)table.get(XHBaseConstant.TABLE_DESC_MEM_STORE_SIZE);
				if(!mem_store_size.isEmpty()){
					td.setMemStoreFlushSize(Long.valueOf(mem_store_size));
				}				
			}
			
			if(table.containsKey(XHBaseConstant.TABLE_DESC_MAX_FILE_SIZE)){
				String max_file_size = (String)table.get(XHBaseConstant.TABLE_DESC_MAX_FILE_SIZE);
				if(!max_file_size.isEmpty()){
					td.setMaxFileSize(Long.valueOf(max_file_size).longValue());
				}				
			}
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return td;
						
	}

	/**
	 * 	  "name":"",
	  "compression":"",
	  "versions":"",
	  "min_versions":"",
	  "blocksize":"",
	  "in_memory":"",
	  "blockcache":"",
	  "bloomfilter":"",
	  "replication_scope":""
	 * @param hcd
	 * @param family
	 * @return
	 */
	private HColumnDescriptor getColumnDescriptor( JSONObject family){
		HColumnDescriptor hcd = null;
		if(family == null)
			return hcd;		
		try{
			if(family.containsKey(XHBaseConstant.TABLE_DESC_FNAME)){
				hcd = new HColumnDescriptor((String)family.get(XHBaseConstant.TABLE_DESC_FNAME));	
			}else{
				return hcd;
			}
						
			// set compression
			if(family.containsKey(XHBaseConstant.TABLE_DESC_COMPRESSION)){
				String compression = (String)family.get(XHBaseConstant.TABLE_DESC_COMPRESSION);
				if(!compression.isEmpty()){				
					if(compression.equalsIgnoreCase("gz")){
						hcd.setCompressionType(Compression.Algorithm.GZ);	
					}else if(compression.equalsIgnoreCase("lzo")){
						hcd.setCompressionType(Compression.Algorithm.LZO);
					}				
				}	
			}
			
			// set versions
			if(family.containsKey(XHBaseConstant.TABLE_DESC_VERSIONS)){
				String versions = (String)family.get(XHBaseConstant.TABLE_DESC_VERSIONS);
				if(!versions.isEmpty()){
					hcd.setMaxVersions(Integer.valueOf(versions));
				}	
			}
			
			// set min versions
			if(family.containsKey(XHBaseConstant.TABLE_DESC_MINVERSIONS)){
				String min_versions = (String)family.get(XHBaseConstant.TABLE_DESC_MINVERSIONS);
				if(!min_versions.isEmpty()){
					hcd.setMinVersions(Integer.valueOf(min_versions));
				}	
			}
			
			
			// set block size
			if(family.containsKey(XHBaseConstant.TABLE_DESC_BLOCKSIZE)){
				String block_size = (String)family.get(XHBaseConstant.TABLE_DESC_BLOCKSIZE);
				if(!block_size.isEmpty()){
					hcd.setBlocksize(Integer.valueOf(block_size));
				}	
			}
			
			
			// set in memory
			if(family.containsKey(XHBaseConstant.TABLE_DESC_INMEMORY)){
				String in_memory = (String)family.get(XHBaseConstant.TABLE_DESC_INMEMORY);
				if(!in_memory.isEmpty()){				
					hcd.setInMemory(in_memory.equalsIgnoreCase("true")? true:false);
				}	
			}
			
			
			// set block cache
			if(family.containsKey(XHBaseConstant.TABLE_DESC_BLOCKCACHE)){
				String block_cache = (String)family.get(XHBaseConstant.TABLE_DESC_BLOCKCACHE);
				if(!block_cache.isEmpty()){
					hcd.setBlockCacheEnabled(block_cache.equalsIgnoreCase("true")? true:false);
				}	
			}
			
			// set bloom filter
			if(family.containsKey(XHBaseConstant.TABLE_DESC_BLOOMFILTER)){
				String bloom_filter = (String)family.get(XHBaseConstant.TABLE_DESC_BLOOMFILTER);
				if(!bloom_filter.isEmpty()){
					if(bloom_filter.equalsIgnoreCase("none")){
						hcd.setBloomFilterType(BloomType.NONE);
					}else if(bloom_filter.equalsIgnoreCase("row")){
						hcd.setBloomFilterType(BloomType.ROW);
					}else if(bloom_filter.equalsIgnoreCase("ROWCOL"))
						hcd.setBloomFilterType(BloomType.ROWCOL); // TODO what is it?
				}	
			}
			
			// set replication scope
			if(family.containsKey(XHBaseConstant.TABLE_DESC_REPLICATIONSCOPE)){
				String replica_scope = (String)family.get(XHBaseConstant.TABLE_DESC_REPLICATIONSCOPE);
				if(!replica_scope.isEmpty()){
					hcd.setScope(Integer.valueOf(replica_scope));
				}	
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return hcd;
	}
	
	
	
	
	private CompareOp matchCompareOperation(String compareOp) throws Exception{
		CompareOp operator = null;
		if (compareOp == null)
			throw new Exception("the compare operation is invalid");
		
		try{
			if(compareOp.equals("=")){
				operator = CompareFilter.CompareOp.EQUAL;
			}else if(compareOp.equals(">")){
				operator = CompareFilter.CompareOp.GREATER;
			}else if(compareOp.equals("<")){
				operator = CompareFilter.CompareOp.LESS;
			}else if(compareOp.equals(">=")){
				operator = CompareFilter.CompareOp.GREATER_OR_EQUAL;
			}else if(compareOp.equals("<=")){
				operator = CompareFilter.CompareOp.LESS_OR_EQUAL;
			}else{
				throw new Exception("The compare operation: "+compareOp+" is invalid");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return operator;
	}
	/**
	 * row filter
	 * @param compareOp
	 * @param regex
	 * @return
	 * @throws Exception
	 */
	public Filter getSubStringFilter(String compareOp, String regex) throws Exception{
		CompareOp operator = matchCompareOperation(compareOp);		
		return new RowFilter(operator,new SubstringComparator(regex));
	}
	
	public Filter getBinaryFilter(String compareOp, String rowkey) throws Exception{
		CompareOp operator = matchCompareOperation(compareOp);		
		return new RowFilter(operator,new BinaryComparator(rowkey.getBytes()));
	}
	/**
	 * Column Range Filter
	 * @param prefix
	 * @return
	 * @throws Exception
	 */
	public Filter getColumnRangeFilter(byte[] minColumn, boolean minColumnInclusive, byte[] maxColumn, boolean maxColumnInclusive) throws Exception{		
		return new ColumnRangeFilter(minColumn,minColumnInclusive,maxColumn,maxColumnInclusive);
	}
	
	
	public Filter getPrefixFilter(String prefix) throws Exception{		
		return new PrefixFilter(prefix.getBytes());
	}
	
	public Filter getRegrexRowFilter(String compareOp, String regex) throws Exception{
		CompareOp operator = matchCompareOperation(compareOp);			
		return new RowFilter(operator,new RegexStringComparator(regex));
	}	

	// Note: there is a jar file from google: google-collections-0.8.jar need to be imported.
	public Filter getTimeStampFilter(List<Long> timestamps) throws Exception{
				
		if (timestamps == null || timestamps.size()<=0)
			throw new Exception("the timestamps list is null");
		
		return new TimestampsFilter(timestamps);	
				
	}	
	/*
	 * access the first column
	 */
	public Filter getFirstColumnFilter() throws Exception{
		return new FirstKeyOnlyFilter();
	}
	
	public Filter getInclusiveFilter(String stopRow) throws Exception{
		return new InclusiveStopFilter(Bytes.toBytes(stopRow));
	}
	
	/*
	 * only access keys
	 */
	public Filter getKeyOnlyFilter() throws Exception {
		return new KeyOnlyFilter();
	}
	
	/**
	 * It should be noticed that the stop row in scan is not included as default
	 * @param rowRange
	 * @param filterList
	 * @param family
	 * @param columns
	 * @param maxVersion
	 * @return
	 * @throws Exception
	 */
	public ResultScanner getResultSet(String[] rowRange,FilterList filterList,String[] family,String[] columns,int maxVersion) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		if(cacheSize < 0)
			throw new Exception("should set cache size before scanning");
		
		Scan scan = null;
		ResultScanner rscanner = null;
		
		try{
			scan = new Scan();
			
			scan.setCaching(this.cacheSize);
			scan.setCacheBlocks(blockCached);
			scan.setFilter(filterList);	
			if(maxVersion>0)
				scan.setMaxVersions(maxVersion);
			
			// scan exclude the stop row directly, so have to make a little difference of the stop row 
			if(rowRange != null){
				scan.setStartRow(rowRange[0].getBytes());
				if(rowRange.length == 2 && rowRange[1] != null)
					scan.setStopRow((rowRange[1]).getBytes());			
			}	
			
			if(columns != null){
				for(int i=0;i<columns.length;i++){
					scan.addColumn(family[0].getBytes(),columns[i].getBytes());	
				}	
			}			

			rscanner = this.table.getScanner(scan);			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return rscanner;
		
	}
	
	public Scan generateScan(String[] rowRange,FilterList filterList,String[] family,String[] columns,int maxVersion) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		if(cacheSize < 0)
			throw new Exception("should set cache size before scanning");
		
		Scan scan = null;		
		
		try{
			scan = new Scan();
			scan.setCaching(this.cacheSize);
			scan.setCacheBlocks(this.blockCached);
			scan.setFilter(filterList);
			if(maxVersion>0)
				scan.setMaxVersions(maxVersion);
			if(rowRange != null){
				scan.setStartRow(rowRange[0].getBytes());
				if(rowRange.length == 2)
					scan.setStopRow(rowRange[1].getBytes());			
			}
				
			if(columns != null){
				for(int i=0;i<columns.length;i++){
					scan.addColumn(family[0].getBytes(),columns[i].getBytes());	
					//System.out.println(family[i]+";"+columns[i]);
				}	
			}							
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return scan;
		
	}	


	public HBaseAdmin getAdmin() {
		return admin;
	}
	

}