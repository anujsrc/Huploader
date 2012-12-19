package com.query.experiment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import com.query.QueryAbstraction;
import com.query.bixi.BixiQuery4QT;
import com.query.bixi.BixiQuery4Raster;
import com.util.XConstants;
import com.util.XTableSchema;

public class QueryClient {
	
	QueryAbstraction queryEngine;
	Properties tests;
	
	public QueryClient(String table_desc_file,String csv_desc_file,String hbase_conf) {		
		
		tests = new Properties();
		try {
			if(!new File("tests.properties").exists()){
				tests.load(new FileInputStream("./cmd/tests.properties"));
			}else{
				 tests.load(new FileInputStream("tests.properties"));
			}
			
			this.queryEngine = this.getQueryEngine(table_desc_file, csv_desc_file, hbase_conf);
		} catch (IOException e) {
			e.printStackTrace();
		}							
				
	}
	
	public static void main(String[] args){
						
		if(args.length < 6){
			System.out.println(
					"schema_desc_file \n " +
					"csv_desc_file \n" +
		            "hbase_conf \n"+
		            "query (0:within,1:point,2:knn) \n"+
		            "cop/scan(0:scan,1:cop) "+
            		"propertyname");
			return;
		}
		QueryClient client = new QueryClient(args[0], args[1], args[2]);		
		
		int query = Integer.valueOf(args[3]);
		int implement = Integer.valueOf(args[4]);
		String property = args[5];
			
		if(query == 0){ // within distance
			if(implement == 0){ // scan
				client.callScanQueryAvailable(property);
			}else if(implement == 1){
				client.callCopQueryAvailable(property);
			}
			
		}else if(query == 1){ // point query
			
			if(implement == 0){// scan
				client.callScanQueryPoint(property);
			}else if(implement == 1){
				client.callCopQueryPoint(property);
			}		
			
		}else if(query == 2){ // knn
			if(implement == 0){ // scan
				client.callScanQueryKNN(property);
			}else if(implement == 1){
				client.callCopQueryKNN(property);
			}			
			
		}else{
			System.out.println("query parameter is wrong");
		}
	}
	
	
	/**
	 * Get the instance for query 
	 * @param table_desc_file
	 * @param csv_desc_file
	 * @param hbase_conf
	 * @return
	 */
	private QueryAbstraction getQueryEngine(String table_desc_file,String csv_desc_file,String hbase_conf){
		
		if(queryEngine == null){
			XTableSchema tableSchema = new XTableSchema(table_desc_file);
			if(tableSchema.getIndexing() == XConstants.INDEX_QUAD_TREE){
				queryEngine = new BixiQuery4QT(tableSchema,csv_desc_file,hbase_conf);
			}else if(tableSchema.getIndexing() == XConstants.INDEX_RASTER){
				queryEngine = new BixiQuery4Raster(tableSchema,csv_desc_file,hbase_conf);
			}
		}	
		return queryEngine;
	}
	

	/**Location query 1 ***/
	private void callScanQueryAvailable(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		Double radius = Double.parseDouble(args[2]);
		HashMap<String,String> result = this.queryEngine.scanQueryAvailableNear("", latitude, longitude, radius);
		//System.out.println(result.toString());
	}
	
	private void callCopQueryAvailable(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		Double radius = Double.parseDouble(args[2]);

		this.queryEngine.copQueryAvailableNear("", latitude, longitude, radius);		
	}
	/**Location query 2 ***/
	private void callScanQueryPoint(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);		
		
		this.queryEngine.scanQueryPoint(latitude, longitude);			

	}
	private void callCopQueryPoint(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);		
		this.queryEngine.copQueryPoint(latitude, longitude);					
	}
	
	/**Location query 3 ***/
	private void callScanQueryKNN(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		int k = Integer.parseInt(args[2]);	
		this.queryEngine.scanQueryAvailableKNN("", latitude, longitude, k);			
	}
	private void callCopQueryKNN(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		int k = Integer.parseInt(args[2]);	
		this.queryEngine.copQueryAvailableKNN("", latitude, longitude, k);		
		
	}		
	
	

	

}
