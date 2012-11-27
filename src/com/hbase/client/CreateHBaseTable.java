package com.hbase.client;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.hbase.service.HBaseUtil;


/**
 * Input: a description of your table,
 * output: a table is created in HBase 
 * @author dan
 *
 */
public class CreateHBaseTable {

	HBaseUtil hbase_service = null;
	/**
	 * @param confPath hbase customized configuration 
	 */
	public CreateHBaseTable(String confPath){
		hbase_service = new HBaseUtil(confPath);
	}
	
	
	public boolean createTable(String schemaFile){
		JSONObject table = this.getTableDescription(schemaFile);
		return hbase_service.createTable(table);
	}
		
	/**
	 * get the table description from the file and return a JSONObject
	 * @param schemaFile
	 * @return
	 */
	private JSONObject getTableDescription(String schemaFile){		
		try{		
			File file = new File(schemaFile);
			if(file.exists()){
				
				FileReader fr = new FileReader(file);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject)parser.parse(fr);							
				
				return obj;
				
			}else{
				System.out.println("file not exist");
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
		
	
	public static void main(String[] args){
		
		if(args.length<2){
			System.out.println("<tableDesscription>,<hbase Configuration>");
			return;
		}
		String tableDesc = args[0];
		String hbaseConf = args[1];
		CreateHBaseTable generation = new CreateHBaseTable(hbaseConf);
		generation.createTable(tableDesc);			
		
	}
}
