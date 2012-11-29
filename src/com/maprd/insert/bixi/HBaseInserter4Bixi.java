package com.maprd.insert.bixi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.maprd.insert.HBaseInserter;
import com.util.XConstants;
import com.util.XHBaseConstant;
import com.util.XParser;

public class HBaseInserter4Bixi extends HBaseInserter{

	public HBaseInserter4Bixi(String input_dir, String input_desc_file,String schema_desc_file){
		super(input_dir,input_desc_file, schema_desc_file);
	}
	
	/*
	 * Map Job to read the data in HDFS and insert them into HBase
	 */
	public static class MapInserter extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

		
		ArrayList<String> input_items = null;
		JSONObject table_schema = null;
		int indexing = -1;
		int subspace = -1;
		int encoding = -1;
		String table_name = null;
		String family = null; // this assumes that there is only one family, but need to be improved later
		
		@Override
		public void configure(JobConf job) {
			input_items = XParser.getTemplateDescAsArray(job.get("input_desc_file"));
			table_schema = XParser.getTableDescription(job.get("schema_desc_file"));
			if(table_schema.containsKey(XHBaseConstant.TABLE_DESC_NAME)){
				String value = (String)table_schema.get(XHBaseConstant.TABLE_DESC_NAME);
				if(!value.isEmpty()){
					this.table_name = value;
				}				
			}
			
			if(table_schema.containsKey(XHBaseConstant.TABLE_DESC_FAMILIES)){
				JSONArray families = (JSONArray)table_schema.get(XHBaseConstant.TABLE_DESC_FAMILIES);
				if(families != null){
					this.family = (String)((JSONObject)families.get(0)).get(XHBaseConstant.TABLE_DESC_FNAME);
				}				
			}else{
				//throws new Exception();
			}
			
			if(table_schema.containsKey(XHBaseConstant.TABLE_DESC_SCHEMA)){
				JSONObject schema = (JSONObject)table_schema.get(XHBaseConstant.TABLE_DESC_SCHEMA);
				
				if(schema.containsKey(XHBaseConstant.TABLE_DESC_INDEXING)){
					String value = (String)schema.get(XHBaseConstant.TABLE_DESC_INDEXING);
					if(!value.isEmpty()){
						this.indexing = Integer.valueOf(value);
					}				
				}
				if(schema.containsKey(XHBaseConstant.TABLE_DESC_ENCODING)){
					String value = (String)schema.get(XHBaseConstant.TABLE_DESC_ENCODING);
					if(!value.isEmpty()){
						this.encoding = Integer.valueOf(value);
					}					
				}
				if(schema.containsKey(XHBaseConstant.TABLE_DESC_SUBSPACE)){
					String value = (String)schema.get(XHBaseConstant.TABLE_DESC_SUBSPACE);
					if(!value.isEmpty()){
						this.subspace = Integer.valueOf(value);
					}		
				}
				if(schema.containsKey(XHBaseConstant.TABLE_DESC_SUBSPACE)){
					String value = (String)schema.get(XHBaseConstant.TABLE_DESC_SUBSPACE);
					if(!value.isEmpty()){
						this.subspace = Integer.valueOf(value);
					}		
				}				
				
			}else{
				//new Throws("the table schema is wrong");
			}
			
		}

		@Override
		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line,",");
			int i = 0;
			String[] values = new String[this.input_items.size()];
			while(tokens.hasMoreTokens()){
				String item = tokens.nextToken();
				values[i++] = item;
			}
			
			if(this.indexing == XConstants.INDEX_QUAD_TREE){
			
				
				
				
			}else if(this.indexing == XConstants.INDEX_RASTER){
				
				
			}
			
			
			// get the value
			// Stringtokneizer, get each value, map to the item
			// get the location and indexing 
			// if(quadtree)
			// indexing, then input
			// else 
			// inexing wiht raster
			// add into put 
			// count the number load 
			// table.put() to HBase
		}			
			
	}

	public void execute() throws Exception{
		JobConf conf = new JobConf(HBaseInserter4Bixi.class);
		conf.setJobName("HBaseInserter4Bixi");
		conf.set("input_desc_file", this.input_desc_file);
		conf.set("schema_desc_file", this.schema_desc_file);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(MapInserter.class);
		
		// set the hbase corresponding class
		//conf.setCombinerClass(Reduce.class);
		//conf.setReducerClass(Reduce.class);
		// set the hbase corresponding class
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		
		
		FileInputFormat.setInputPaths(conf,new Path(this.input_dir));
		//FileOutputFormat.setOutputPath(conf, new Path(out_dir));
		
		JobClient.runJob(conf);
	
	}
	
	
	public static void main(String[] args) {
		String input = "/example";
		String input_desc = "/conf/bixi.conf";
		String schema= "/template/bixi1.schema";
		HBaseInserter4Bixi bixi = new HBaseInserter4Bixi(input,input_desc,schema);
		try{
			bixi.execute();	
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	
	
}
