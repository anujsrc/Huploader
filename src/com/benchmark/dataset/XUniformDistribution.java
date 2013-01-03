package com.benchmark.dataset;

import java.io.FileWriter;
import java.util.Random;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomDataImpl;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Generate the experimented data of normal distribution
 * 
 * @author dan
 *
 */
public class XUniformDistribution {

	String fileNamePrefix = "uniform";
	
	public String generate(int number,int min, int max){	
		
		
		double x[] = new double[number];
		double y[] = new double[number];
					
		int total = 0;
	
		RandomDataImpl randomData1 = new RandomDataImpl(); 
		RandomDataImpl randomData2 = new RandomDataImpl();	
		randomData2.reSeed(10);
		randomData1.reSeed(1);
		for(int i=0;i<number;i++){										
			x[total] = randomData1.nextUniform(0, number);
			y[total] = randomData2.nextUniform(0, number);							
			//System.out.println(x[total]+","+y[total]+"==="+x[total] / (number/max)+";"+y[total] / (number/max));
			total++;																
		}
		
		System.out.println(total);
		// normailze the value into the given value range
		System.out.println("===normalized==and start to write=");
		FileWriter fw = null;
		try{
			fw = new FileWriter(fileNamePrefix+".csv");
			for(int i=0;i<total;i++){
				x[i] = x[i] / (number/max);
				y[i] = y[i] / (number/max); 
				fw.write(x[i] + ","+y[i]+"\n");
				
			}
			fw.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return fileNamePrefix+".csv";
	}	
	
	public static void main(String []args){
		XUniformDistribution distribution = new XUniformDistribution();
		distribution.generate(10000000,0,100);
	}	
	
	
	
	
	
	
	
	
	
	
	
	
/**
 * create the simulated data
 * @param folder: where the file would be stored
 * @param fileName: formulated file name
 * @param partitions: number of files
 * @param number: number of record in each file
 */
	public void create(String folder,String fileName,int partitions, int number){
		
		String[] metrics = new String[]{"installed","locked","installDate","removeDate","temporary","nbBikes","nbEmptyDocks"}; 
		try{
			// 
			for(int i=0;i<partitions;i++){

				// new a file
				for(int j=0;j<number; j++){
					

					double latitude = 1; // generate lat
					double longitude = 1; // generate longitude
					String id = "id";
					String name = "";
					// generate others
					// write the record to 
					
				}// finish all record created 
				
				// close the file 
				
				
				
				
				
			}
			
			
/*			Random latRandom = new Random();
			latRandom.setSeed(1);
			double latStart = 0;
			double width = 100;
			double longStart = 0;
			double total_height = 100;
			for(int j=0;j<parts;j++){
				Random longRandom = new Random();
				longRandom.setSeed(10000*j);
				longStart = j % 100;				
				double height = total_height/parts;
				Document document  = db.newDocument();			
				
				// create root node
				Element stations = document.createElement("stations");
				// 1M points
				for(int i=0;i<100;i++){					
					// create data node
					Element station = document.createElement("station");
					// set data attribute there is no attribute for the element
					// create a child
					// append the child to the parent
					Element ele = document.createElement("id");
					ele.setTextContent("s"+j+"-"+i);
					station.appendChild(ele);	

					Element name = document.createElement("terminalName");
					name.setTextContent("name"+i);
					station.appendChild(name);	
					
					Element lat = document.createElement("lat");
					// 1st time to generate data
					double latValue = latStart + (latRandom.nextDouble() * width);
					// 2nd time to generate data
					//double latValue = latStart + (latRandom.nextGaussian() * width);
					// 3rd time to generated data 
					//double latValue = latStart + (latRandom.nextFloat() * width);
					lat.setTextContent(String.valueOf(latValue));
					station.appendChild(lat);	
					
					Element longtitude = document.createElement("long");
					// 1st time to generate data
					double longValue = longStart + (longRandom.nextDouble() * height);
					// 2ndtime to generate data					
					//double longValue = longStart + (longRandom.nextGaussian() * height);
					//3rd time to generate data
					//double longValue = longStart + (longRandom.nextFloat() * height);
					
					longtitude.setTextContent(String.valueOf(longValue));				
					station.appendChild(longtitude);	
					
					
					for(int k=0;k<metrics.length;k++){
						Element e = document.createElement(metrics[k]);
						e.setTextContent(latValue+";"+longValue);
						//station.appendChild(e);						
					}
					
					stations.appendChild(station);	
				}	
				document.appendChild(stations);					
				// save the document to output file
				this.save(folder,j+"-"+fileName,document);
				System.out.println(j+"-"+fileName+" is created");*/
			//}
		
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	private void save(String folder, String outfileName,Document document){
		try {			 
			  // create a transformer 
			  TransformerFactory transFactory = TransformerFactory.newInstance();
			  Transformer        transformer  = transFactory.newTransformer();
			   
			  // set some options on the transformer
			  transformer.setOutputProperty(OutputKeys.ENCODING, "utf-8");
			  transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
			  transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			  transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
			 
			  // get the supporting classes for the transformer
			  FileWriter writer = new FileWriter(folder+"/"+outfileName);
			  StreamResult result = new StreamResult(writer);
			  DOMSource    source = new DOMSource(document);
			   
			  // transform the xml document into a string
			  transformer.transform(source, result);
			   
			  // close the output file
			  writer.close();
			   
			} catch(javax.xml.transform.TransformerException e) {
				e.printStackTrace();
				
			}catch (java.io.IOException ex) {
				ex.printStackTrace();			  
			}
	}
	
	
}
