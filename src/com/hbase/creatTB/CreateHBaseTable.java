package com.hbase.creatTB;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.hbase.service.HBaseUtil;
import com.util.XParser;


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
		JSONObject table = XParser.getTableDescription(schemaFile);
		return hbase_service.createTable(table);
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
