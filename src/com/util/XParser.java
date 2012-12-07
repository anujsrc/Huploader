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
				System.out.println(schemaFile+" does not exist");
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * The template of the csv
	 * @param template_file
	 * @return headers in csv and the type of each item
	 */
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
					String pair[] = items[i].split("=>");
					item_set.put(pair[0], pair[1]);
				}
			}else{
				System.out.println(template_file+" does not exist");
			}
				
		}catch(Exception e){
			e.printStackTrace();
		}
		return item_set;
	}
	
	public static String[] getTemplateDescAsArray(String template_file){
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
					String pair[] = items[i].split("=>");
					item_set.add(pair[0]);
				}
			}else{
				System.out.println(template_file+" does not exist");
			}
				
			String[] items = item_set.toArray(new String[item_set.size()]);
			return items;
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return null;
	}	

}
