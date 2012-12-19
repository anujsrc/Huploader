package com.query.bixi;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import com.query.QueryAbstraction;
import com.util.XTableSchema;

public class HybridIndex extends QueryAbstraction{

	
	public HybridIndex(String schema_desc_file,String csv_desc_file,String hbase_conf){
		super(schema_desc_file,csv_desc_file,hbase_conf);
	}
	
	public HybridIndex(XTableSchema schema,String csv_desc_file,String hbase_conf){	
		super(schema,csv_desc_file,hbase_conf);
	}	
	
	@Override
	public List<String> copQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, String> scanQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void copQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TreeMap<Double, String> scanQueryAvailableKNN(String timestamp,
			double latitude, double longitude, int n) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String copQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String scanQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub
		return null;
	}	
	

}
