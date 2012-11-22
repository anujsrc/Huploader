package com.preprocessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Hashtable;
import java.util.Iterator;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class XMLTransformer extends Transformer{

	SAXParserFactory factory = null;
	SAXParser saxParser = null;
	
	Hashtable<String, String> item_set = null;
	
	public XMLTransformer(){
		try{
			this.factory = SAXParserFactory.newInstance();
			saxParser = factory.newSAXParser();
			item_set = new Hashtable<String,String>();
		}catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	/**
	 * 
	 * @param inputDir
	 * @param format
	 * @param template
	 * @param outDir
	 */
	public void execute(String inputDir, int format, String template, String outDir){
		
		this.getTemplate(template);
		
		File in_directory = new File(inputDir);
		File out_directory = new File(outDir);
		try{
			if(!out_directory.exists())
				out_directory.createNewFile();
			if(in_directory.exists()){			
				File[] files = in_directory.listFiles();
				
				for(int i =0;i<files.length;i++){
					String name = files[i].getName();
					if(name.contains(".xml")){
						this.parseXML(in_directory.getAbsolutePath()+"/"+name,out_directory.getAbsolutePath()+"/"+name+".csv");	
					}				
					
				}
			}	
		}catch(Exception e){
			e.printStackTrace();
		}
		
				
	}
	
	private void getTemplate(String template_file){
		
		try{
			File file = new File(template_file);
			if(file.exists()){
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
	}
	
	/**
	 * Parse the xml file with JAX
	 * Read the location from file, parse it, index it, and then insert it
	 * @param filename            
	 */
	private void parseXML(String inFile,String outFile) {
		try {			 		
			// create the handler
			MyHandler handler = new MyHandler(this.item_set, outFile);
								
			this.saxParser.parse(inFile, handler);
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}	
	

	class MyHandler extends DefaultHandler{
				
		int count = 0;
			
		FileWriter writer = null;
		String[] keys = null;
		boolean[] indicator = null;
		String[] values = null;
		
		public MyHandler(Hashtable<String,String> item_sets,String outputFile){			
			int len = item_sets.keySet().size();
			keys = new String[len];
			indicator = new boolean[len];
			values = new String[len];
			int index = 0;
			Iterator<String> items = item_sets.keySet().iterator();
			while(items.hasNext()){
				keys[index] = items.next();
				indicator[index] = false;
				values[index] = null;
				index++;
			}
			// open the write file
			try{
				writer = new FileWriter(outputFile,true);	
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}		
		
		public void startElement(String uri, String localName, String qName, 
			Attributes attributes) throws SAXException{
		
			for(int i = 0;i<keys.length;i++){
				if(qName.equals(keys[i])){
					this.indicator[i] = true;
					break;
				}
			}
		
		}
	
	public void endElement(String uri, String localName, String qName) throws SAXException{
		//System.out.println("end Element: "+qName);
		if(qName.equalsIgnoreCase("station")){
			// write to the file
			String str = "";
			for(int i=0;i<values.length;i++){
				str += values[i];
				if(i<values.length-1)
					str += ",";
			}
			str += "\n";
			try{
				this.writer.write(str);	
			}catch(Exception e){
				e.printStackTrace();
			}			
			count++;

		}else if (qName.equalsIgnoreCase("stations")){
			// close the file
			try{
				this.writer.close();
			}catch(Exception e){
				e.printStackTrace();
			}
			System.out.println("insert number: "+count);
		}
	}
	
	public void characters(char ch[], int start, int length) throws SAXException{
		
		String character = new String(ch, start, length);
		
		for(int i=0;i<indicator.length;i++){
			if(indicator[i]){
				values[i] = character;
				System.out.println(character+ "; "+ values[i] );
				indicator[i] = false;
				break;
			}
		}		
		}		
	}
		
}
