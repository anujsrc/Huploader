package com.query.bixi;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import com.query.QueryAbstraction;
import com.query.bixi.coprocessor.BixiProtocol;
import com.query.bixi.coprocessor.RCopResult;
import com.util.XCSVFormat;
import com.util.XTableSchema;
import com.util.quadtree.trie.XQuadTree;

/**
 * Query with QuadTree indexing
 * @author dan
 *
 */
public class BixiQuery4QT extends QueryAbstraction{

	public BixiQuery4QT(String schema_desc_file,String csv_desc_file,String hbase_conf){
		super(schema_desc_file,csv_desc_file,hbase_conf);
	}
	
	public BixiQuery4QT(XTableSchema schema,String csv_desc_file,String hbase_conf){	
		super(schema,csv_desc_file,hbase_conf);
	}
	
	@Override
	public List<String> copQueryAvailableNear(String timestamp,
			final double latitude, final double longitude,final double radius) {
		
		try{
			// create the log file
			this.getCSVLog(0); 
			this.getCSVLog(1);			
			//
			this.timePhase.clear();
		    /**Step1** Call back class definition **/

		    class BixiCallBack implements Batch.Callback< RCopResult> {
		    	RCopResult res = new RCopResult();
		    	int count = 0; // the number of coprocessor
		    	QueryAbstraction query = null;
		    	
		     public BixiCallBack(QueryAbstraction query){
		    	this.query = query; 
		     }
		      @Override
		      public void update(byte[] region, byte[] row,  RCopResult result) {
		    	  long current = System.currentTimeMillis();
		    	  count++;		    	 
		    	  res.getRes().addAll(result.getRes()); // to verify the error when large data
		    	  res.setStart(result.getStart());
		    	  res.setEnd(result.getEnd());
		    	  res.setRows((res.getRows()+result.getRows()));	
		    	  res.setCells(res.getCells()+result.getCells());
		    	  // write them into csv file
		    	  String outStr = "";
		    	  outStr += "within,"+"cop,"+result.getParameter()+","+result.getStart()+","+
		    			  	result.getEnd()+","+current+","+
		    			  	result.getRows()+","+result.getCells()+","+result.getKvLength()+","+result.getRes().size()+","+
		    			  	this.query.regionAndRS.get(Bytes.toString(region))+","+Bytes.toString(region);		    			  	
		    	  this.query.writeCSVLog(outStr,1);
		    	  		    	  
		      }		      
		    }
		    
		    BixiCallBack callBack = new BixiCallBack(this);
		    
		    /**Step2*** generate scan***/ 
			// build up a quadtree.
			long s_time = System.currentTimeMillis();
			this.timePhase.add(s_time);			
			
			double x = latitude - radius;
			double y = longitude - radius;								   
			// match rect to find the subspace it belongs to
			
			long match_s = System.currentTimeMillis();
			List<String> indexes = this.quadTree.match(x,y,2*radius,2*radius);
			long match_time = System.currentTimeMillis() - match_s;			
			
			// prepare filter for scan
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			
			for(String s:indexes){								
				if(s!=null){
					Filter rowFilter = hbase.getPrefixFilter(s);	
					fList.addFilter(rowFilter);	
				}				
			}			
	    	
			Object[] objs = indexes.toArray();
	    	Arrays.sort(objs);
			String[] rowRanges= new String[2];
	    	rowRanges[0] = (String)objs[0];
	    	rowRanges[1] = (String)objs[objs.length-1]+"-*";
				    		    	
	    		    	 		   
	    	final Scan scan = hbase.generateScan(rowRanges,fList, null,null,-1);		    
		    		    
		    System.out.println("start to send the query to coprocessor.....");		    
		    
		    /**Step3: send request to trigger Coprocessor execution**/		    
		    long cop_start = System.currentTimeMillis();		    
		    this.timePhase.add(cop_start);
		    
		    final XCSVFormat csv = this.csvFormat;
		    hbase.getHTable().coprocessorExec(BixiProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<BixiProtocol,  RCopResult >() {
		      
		    	public  RCopResult call(BixiProtocol instance)
		          throws IOException {
		    		
		        return instance.copQueryNeighbor4QT(scan,latitude,longitude,radius,csv);			        
		        
		      };
		    }, callBack);
		    
		    long cop_end = System.currentTimeMillis();
		    this.timePhase.add(cop_end);				   			    
		    
			long exe_time = cop_end- s_time; 	
			// write to csv file
			String outStr = "";
			outStr += "within,"+"cop,"+callBack.res.getRes().size()+","+callBack.res.getCells()+","+callBack.res.getRows()+","+
						exe_time+","+match_time+","+this.tableSchema.getSubSpace()+","+radius;	
					
			
						
			for(int i=0;i<this.timePhase.size();i++){				
				outStr += ",";
				outStr += this.timePhase.get(i);				
			}	
			this.writeCSVLog(outStr, 0);
			// write it to csv file
			//this.writeCSVLog(timeStr,2);
			
		    return callBack.res.getRes();			
			
			
			
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbase.closeTableHandler();						
			this.closeCSVLog();			
		}
			
		return null;
	}

	@Override
	public HashMap<String, String> scanQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		long sTime = System.currentTimeMillis();

		this.timePhase.clear();
		
		this.timePhase.add(System.currentTimeMillis());
		longitude = Math.abs(longitude);
		double x = latitude - radius;
		double y = longitude - radius;
		Point2D.Double point = new Point2D.Double(latitude,longitude);
		ResultScanner rScanner = null;
		//result container
		HashMap<String,String> results = new HashMap<String,String>();
		try{
			this.getCSVLog(0);		
			//this.getCSVLog(2);
			
			// match rect to find the subspace it belongs to
			long match_s = System.currentTimeMillis();
			List<String> indexes = quadTree.match(x,y,2*radius,2*radius);
			long match_time = System.currentTimeMillis() - match_s;
			// prepare filter for scan
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String s:indexes){			
				if(s!=null){
					Filter rowFilter = hbase.getPrefixFilter(s);	
					fList.addFilter(rowFilter);	
				}				
			}	
	    	Object[] objs = indexes.toArray();
	    	Arrays.sort(objs);
	    	String[] rowRanges= new String[2];
	    	rowRanges[0] = (String)objs[0];
	    	rowRanges[1] = (String)objs[objs.length-1]+"-*";
			
	    	this.timePhase.add(System.currentTimeMillis());
			rScanner = this.hbase.getResultSet(rowRanges,fList, null,null,-1);			
			int count = 0;
			int accepted = 0;
			int row = 0;
			for(Result r: rScanner){
				//System.out.println(Bytes.toString(r.getRow()) + "=>");
				List<KeyValue> pairs = r.list();
				row++;
				for(KeyValue kv:pairs){
					//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
					count++;
					// get the distance between this point and the given point					
					Hashtable<String,String> key_value = this.csvFormat.fromPairString(Bytes.toString(kv.getValue()));					
					
					Point2D.Double resPoint = new Point2D.Double(Double.valueOf(key_value.get("lat")).doubleValue(),
								Double.valueOf(key_value.get("long")).doubleValue());
					double distance = resPoint.distance(point);
					
					if(distance <= radius){						
						//System.out.println("row=>"+Bytes.toString(r.getRow()) + ";colum=>"+Bytes.toString(kv.getQualifier())+ ";station=>"+station.getId());
						results.put(Bytes.toString(kv.getQualifier()), String.valueOf(distance));
						accepted++;
					}
						
				}
			}
			long eTime = System.currentTimeMillis();	
			this.timePhase.add(eTime);

			
			// write to csv file
			String outStr = "";
			outStr += "within,"+"scan,"+accepted+","+count+","+row+","+(eTime-sTime)+
					  ","+match_time+","+this.tableSchema.getSubSpace()+","+radius;	
					
			
			
			//String timeStr = "within,scan,"+radius+",";			
			for(int i=0;i<this.timePhase.size();i++){
				outStr+=",";
				outStr += this.timePhase.get(i);				
			}
			this.writeCSVLog(outStr, 0);						
			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			this.hbase.closeTableHandler();			
			this.closeCSVLog();
		}
		return results;
	}

	@Override
	public String copQueryPoint(final double latitude, final double longitude) {
		
		try{	
			this.getCSVLog(0);		
			this.getCSVLog(2);	
			this.timePhase.clear();		
		    /**Step1** Call back class definition **/
		    class BixiCallBack implements Batch.Callback< RCopResult> {
		    	RCopResult res = new RCopResult();
		    	int count = 0;
		    	QueryAbstraction query = null;
		    	
		     public BixiCallBack(QueryAbstraction query){
		    	this.query = query; 
		     }
		      @Override
		      public void update(byte[] region, byte[] row,  RCopResult result) {
		    	  count++;
		    	  
					long current = System.currentTimeMillis();
					count++;
					if(result.getRes() != null)
						res.getRes().addAll(result.getRes()); // to verify the error when large data
					res.setStart(result.getStart());
					res.setEnd(result.getEnd());
					res.setRows((res.getRows()+result.getRows()));
					res.setCells(res.getCells()+result.getCells());								
			    	  // write them into csv file
			    	 String outStr = "";
			    	  outStr += "within,"+"cop,"+result.getParameter()+","+result.getStart()+","+result.getEnd()+","+current+","+
			    			  	result.getRows()+","+result.getCells()+","+result.getKvLength()+","+result.getRes().size()+","+
			    			  	this.query.regionAndRS.get(Bytes.toString(region))+","+Bytes.toString(region);
			    	  this.query.writeCSVLog(outStr,1);								    	
				}		    	  
		    	  		    	  
		      }		      		    
		    BixiCallBack callBack = new BixiCallBack(this);		   
		    
		    /**Step2*** generate scan***/							   
			// match rect to find the subspace it belongs to
		    long s_time = System.currentTimeMillis();
			this.timePhase.add(s_time);
			
			long match_s = System.currentTimeMillis();
			XQuadTree node = quadTree.locate(latitude, longitude);
			long match_time = System.currentTimeMillis() - match_s;			
			
	    	String[] rowRanges= new String[2];
	    	rowRanges[0] = node.getIndex();
	    	rowRanges[1] = node.getIndex()+"-*";
				    	
		    final Scan scan = hbase.generateScan(rowRanges,null, null,null,-1);		    		    		   
		    
		    System.out.println("start to send the query to coprocessor.....");		    
		    
		    /**Step3: send request to trigger Coprocessor execution**/
		    this.timePhase.add(System.currentTimeMillis());
		    final XCSVFormat csv = this.csvFormat;
		    hbase.getHTable().coprocessorExec(BixiProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<BixiProtocol,  RCopResult >() {
		      
		    	public  RCopResult call(BixiProtocol instance)
		          throws IOException {
		    		
		        return instance.copQueryPoint4QT(scan, latitude, longitude,csv);			        
		        
		      };
		    }, callBack);

		    long e_time = System.currentTimeMillis();
		    this.timePhase.add(e_time);		    					
			
		    long exe_time = e_time - s_time;
			// write to csv file
			String outStr = "";
			outStr += "within,"+"cop,"+callBack.res.getRes().size()+","+callBack.res.getCells()+","+callBack.res.getRows()+","+
						exe_time+","+match_time+","+this.tableSchema.getSubSpace()+",("+latitude+":"+longitude+")";	
											
			
			for(int i=0;i<this.timePhase.size();i++){
				outStr += ",";
				outStr +=this.timePhase.get(i);
			}					
			this.writeCSVLog(outStr, 0);
			
		    return callBack.res.getRes().get(0);
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbase.closeTableHandler();
			this.closeCSVLog();
		}
		
		return null;	
	}

	@Override
	public String scanQueryPoint(double latitude, double longitude) {
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
	
	

}
