package com.util;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.geom.Point2D.Double;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class XTableSchema {
	
	JSONObject table = null;
	
	public XTableSchema(String schema_desc_file){
		this.table = XParser.getTableDescription(schema_desc_file);
	}
	
	/**
	 * get table name
	 */
	public String getTableName(){
		String name = null;
		try{
			if(this.table != null){
				if(this.table.containsKey(XHBaseConstant.TABLE_DESC_NAME)){
					name = (String)this.table.get(XHBaseConstant.TABLE_DESC_NAME);
				}
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return name;
	}
	/**
	 * Get the log file prefix name
	 * @return
	 */
	
	public String getLogFilePrefix(){
		
		String prefix = null;
		try{
			if(this.table != null){
				if(this.table.containsKey(XHBaseConstant.TABLE_DESC_LOG_NAME_PREFIX)){
					prefix = (String)this.table.get(XHBaseConstant.TABLE_DESC_LOG_NAME_PREFIX);
				}
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return prefix;		
	}
	
	
	/**
	 * get the family item in the table schema 
	 */
	public JSONObject getObject(String key){		
		try{			
			if(this.table != null){
				if(this.table.containsKey(key)){
					return (JSONObject)this.table.get(key);
				}
			}
						
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * get the array in the table schema 
	 */
	public JSONArray getObjectArray(String key){		
		try{			
			if(this.table != null){
				if(this.table.containsKey(key)){
					return (JSONArray)this.table.get(key);
				}
			}
						
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}	
	
	
	/**
	 * For csv file, there is only one family
	 */
	public JSONObject getFamilyObject(){		
		return (JSONObject)(this.getObjectArray(XHBaseConstant.TABLE_DESC_FAMILIES).get(0));		
	}
	/**
	 * get family name
	 * @return
	 */
	public String getFamilyName(){
		JSONObject family = this.getFamilyObject();
		if(family.containsKey(XHBaseConstant.TABLE_DESC_FNAME)){
			return (String)family.get(XHBaseConstant.TABLE_DESC_FNAME);
		}else{
			return null;
		}
		
	}
	
	/**
	 * get max version
	 * @return
	 */
	public int getMaxVersions(){
		JSONObject family = this.getFamilyObject();
		if(family.containsKey(XHBaseConstant.TABLE_DESC_VERSIONS)){
			return Integer.valueOf((String)family.get(XHBaseConstant.TABLE_DESC_VERSIONS));
		}else{
			return 1;
		}
	}	
	
	/**
	 * get the family item in the table schema 
	 */
	public JSONObject getSchemaObject(){		
		return getObject(XHBaseConstant.TABLE_DESC_SCHEMA);		
	}	
	/**
	 * used in indexing.
	 * @return
	 */
	public double getSubSpace(){
		JSONObject schema = getSchemaObject();
		if(schema.containsKey(XHBaseConstant.TABLE_DESC_SUBSPACE)){
			return java.lang.Double.valueOf((String)schema.get(XHBaseConstant.TABLE_DESC_SUBSPACE));
		}else{
			return -1;
		}
	}
	/**
	 * used in indexing.
	 * @return
	 */
	public int getIndexing(){
		JSONObject schema = getSchemaObject();
		if(schema.containsKey(XHBaseConstant.TABLE_DESC_INDEXING)){
			return Integer.valueOf((String)schema.get(XHBaseConstant.TABLE_DESC_INDEXING));
		}else{
			return -1;
		}
	}	
	
	/**
	 * used in indexing.
	 * @return
	 */
	public int getEncoding(){
		JSONObject schema = getSchemaObject();
		if(schema.containsKey(XHBaseConstant.TABLE_DESC_ENCODING)){
			return Integer.valueOf((String)schema.get(XHBaseConstant.TABLE_DESC_ENCODING));
		}else{
			return -1;
		}
	}	
	
	
	
	/**
	 * The entire space, the format is (x-topleft,y-topleft,x-bottomright,y-bottomright)
	 * @return
	 */
	public Rectangle2D.Double getEntireSpace(){
		JSONObject schema = getSchemaObject();
		if(schema.containsKey(XHBaseConstant.TABLE_DESC_SPACE)){
			String space = (String)schema.get(XHBaseConstant.TABLE_DESC_SPACE);
			String[] items = space.split(",");
			return new Rectangle2D.Double(java.lang.Double.valueOf(items[0]).doubleValue(),
											java.lang.Double.valueOf(items[1]).doubleValue(),
											java.lang.Double.valueOf(items[2]).doubleValue(),
											java.lang.Double.valueOf(items[3]).doubleValue());
			
		}else{
			return null;
		}
	}	
	
	/**
	 * The offset point to normalize the space to the first quadrant, 
	 * the format is (x,y)
	 * @return
	 */
	public Point2D.Double getOffset(){
		JSONObject schema = getSchemaObject();
		if(schema.containsKey(XHBaseConstant.TABLE_DESC_OFFSET)){
			String space = (String)schema.get(XHBaseConstant.TABLE_DESC_OFFSET);
			String[] items = space.split(",");
			if(items != null || items.length == 2){
				return new Point2D.Double(java.lang.Double.valueOf(items[0]).doubleValue(),
						java.lang.Double.valueOf(items[1]).doubleValue());
			}
		}
		return null;
	}		
}
