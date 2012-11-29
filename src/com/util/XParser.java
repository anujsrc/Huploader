package com.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Hashtable;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class XParser {
	
	/**
	 * get the table description from the file and return a JSONObject
	 * @param schemaFile
	 * @return
	 */
	public static JSONObject getTableDescription(String schemaFile){		
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
	
	public static Hashtable<String, String> getTemplateDesc(String template_file){
		Hashtable<String,String> item_set = null;
		try{
			File file = new File(template_file);
			if(file.exists()){				
				item_set = new Hashtable<String,String>();
				FileReader reader = new FileReader(file);
				BufferedReader br = new BufferedReader(reader);
				String line = br.readLine();
				String[] items = line.split(",");
				for(int i=0;i<items.length;i++){
					item_set.put(items[i], "");
				}
			}
				
		}catch(Exception e){
			e.printStackTrace();
		}
		return item_set;
	}
	
	public static ArrayList<String> getTemplateDescAsArray(String template_file){
		ArrayList<String> item_set = null;
		try{
			File file = new File(template_file);
			if(file.exists()){				
				item_set = new ArrayList<String>();
				FileReader reader = new FileReader(file);
				BufferedReader br = new BufferedReader(reader);
				String line = br.readLine();
				String[] items = line.split(",");
				for(int i=0;i<items.length;i++){
					item_set.add(items[i]);
				}
			}
				
		}catch(Exception e){
			e.printStackTrace();
		}
		return item_set;
	}	

}
