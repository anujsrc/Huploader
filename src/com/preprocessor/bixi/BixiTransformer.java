package com.preprocessor.bixi;

import java.io.FileWriter;
import java.util.Hashtable;
import java.util.Iterator;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.preprocessor.Transformer;

/**
 * This is can be used as Bixi. Because there is still a hard code..<station> <stations>
 * @author dan
 *
 */
public class BixiTransformer extends Transformer{

	SAXParserFactory factory = null;
	SAXParser saxParser = null;
	
	
	public BixiTransformer(){
		super();
		try{
			this.factory = SAXParserFactory.newInstance();
			saxParser = factory.newSAXParser();			
		}catch(Exception e){
			e.printStackTrace();
		}		
	}

	
	/**
	 * Parse the xml file with JAX
	 * Read the location from file, parse it, index it, and then insert it
	 * @param filename            
	 */
	protected void parse(String inFile,String outFile) {
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
			System.out.println("num_in_file=>"+count);
		}
	}
	
	public void characters(char ch[], int start, int length) throws SAXException{
		
		String character = new String(ch, start, length);
		
		for(int i=0;i<indicator.length;i++){
			if(indicator[i]){
				values[i] = character;				
				indicator[i] = false;
				break;
			}
		}		
		}		
	}
		
}
