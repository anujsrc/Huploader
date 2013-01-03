package com.benchmark.dataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class XBixiGenerator {
	
	int number = 0;
	int min = 0;
	int max = 0;
	
	public XBixiGenerator(int number,int min,int max){
		this.number = number;
		this.min = min;
		this.max = max;
	}
		
	public void generateData(String distributionType, String outputFile) throws Exception{
		
		System.out.println("start to generate data...");
		String distributionFile = null;
		if(distributionType.equals("zipf")){
			XZipfDistribution zipf = new XZipfDistribution();
			distributionFile = zipf.generate(this.number,this.min,this.max);
		}else if(distributionType.equals("uniform")){
			XUniformDistribution uniform = new XUniformDistribution();
			distributionFile = uniform.generate(this.number,this.min,this.max);
		}
		if(distributionFile == null)
			throw new Exception("the distribution file is null! ");
		else
			System.out.println(distributionFile);
		try{
			FileWriter fw = new FileWriter(outputFile);
			FileReader fr = new FileReader(distributionFile);
			BufferedReader br = new BufferedReader(fr);	
			String line = br.readLine();
			System.out.println(line);
			int uid = 0;
			System.out.println("start to generate Bixi data...");
			while(line != null && !line.isEmpty()){
				String[] location = line.split(",");
				String oneBixi = this.getFakeBixi(uid, location);				
				fw.write(oneBixi+"\n");
				uid++;
				line = br.readLine();
			}
			
			fr.close();
			fw.close();
			
			System.out.println("Finish the generation..."+outputFile);
			
		}catch(Exception e){
			e.printStackTrace();
		}						
		
	}

	/**
	 * generate faked bixi with the location information
	 * @param location
	 * @return
	 */
	public String getFakeBixi(int uid, String[] location){
		String oneBixi = "";
		if(location == null)
			return oneBixi;
		// station id
		oneBixi += "s"+uid+",";
		// latitude and longitude		
		oneBixi += location[0]+",";
		oneBixi += location[1]+",";		
		// teminal name
		oneBixi += (100*uid)+",";
		// Name
		oneBixi += "Name"+(100*uid)+",";
		//<nbBikes>4</nbBikes>
		oneBixi += Math.round(Math.random())+",";
		//<nbEmptyDocks>45</nbEmptyDocks>
		oneBixi += Math.round(Math.random())+",";
		
		return oneBixi;
	}
	
	public static void main(String args[]){
				
		if(args.length < 5){
			System.out.println("parameter should be 4");
			return;
		}
			
		String distributionType = args[0];
		int number = Integer.valueOf(args[1]);
		int min = Integer.valueOf(args[2]);
		int max = Integer.valueOf(args[3]);
		String outputFolder = args[4];
		
		XBixiGenerator bixi = new XBixiGenerator(number,min,max);
		try{
			bixi.generateData(distributionType, outputFolder+"/"+distributionType+"-bixi.csv");	
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
