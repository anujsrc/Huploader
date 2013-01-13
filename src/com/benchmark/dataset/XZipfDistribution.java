package com.benchmark.dataset;

import java.io.FileWriter;
import java.util.Hashtable;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomData;
import org.apache.commons.math3.random.RandomDataImpl;
import org.apache.commons.math3.random.RandomGenerator;
/**
 * 1 generate x with the zipf distribution
 * 2 get the x-value frequency, and make it as the number of y-value
 * 3 combine the (x,y) as the coordination of point.
 * If the number of points is 10,000,000, so the input is zipf(10,000,000, 1), but the value is [1,10,000,000],
 * so, in the end, should normalize the value to [0,100) 
 * 
 * @author dan
 *
 */


public class XZipfDistribution {

	String fileNamePrefix = "zipf";
	
	public String generate(int number,int min, int max,String outputFolder){
		
		if(outputFolder != null){
			fileNamePrefix = outputFolder+"/"+fileNamePrefix;
		}
		
		RandomDataImpl randomData1 = new RandomDataImpl(); 
		double x[] = new double[number*2];
		double y[] = new double[number*2];
		double stat_x[] = new double[number];
		double stat_y[] = new double[number];
		for(int i=0;i<number*2;i++){
			x[i] = 0;
			y[i] = 0;
			if(i<number){
				stat_x[i] = 0;
				stat_y[i] = 0;
			}			
		}
		
		
		
		ZipfDistribution zipf = new ZipfDistribution(number,1);		
		int total = 0;
		for(int i=0;i<number;i++){		
			double probability = zipf.probability(i);			
			long count = Math.round((probability*number)+0.5); 
			stat_x[i] = count;
			RandomDataImpl randomData2 = new RandomDataImpl();
			randomData2.reSeed(i);
			randomData1.reSeed(i);
			for(int j=0;j<count;j++){
				x[total] = i+randomData1.nextUniform(0, 1);
				//int yy = (int)(randomData2.nextZipf((int)count, 1)*(number/count));
				int yy = randomData2.nextZipf((int)count, 1);
				if(yy == number)
					yy = yy -1;				
				stat_y[yy]++;
				y[total] = yy+randomData2.nextUniform(0, 1);
				
			//	System.out.println(x[total]+","+y[total]+"==="+x[total] / (number/max)+";"+y[total] / (number/max));
				total++;									
			}
				
		}
		
		System.out.println(total);
		// normailze the value into the given value range
		System.out.println("===normalized==and start to write=");
		FileWriter fw = null;
		FileWriter xstatFile = null;
		FileWriter ystatFile = null;
		try{
			fw = new FileWriter(fileNamePrefix+".csv");
			xstatFile = new FileWriter(fileNamePrefix+"-x.spc");
			ystatFile = new FileWriter(fileNamePrefix+"-y.spc");
			xstatFile.write("m\tVm\n");
			ystatFile.write("m\tVm\n");
			for(int i=0;i<total;i++){
				x[i] = x[i] / (number/max);
				y[i] = y[i] / (number/max); 
				fw.write(x[i] + ","+y[i]+","+"\n");		
				if(i<number){
					xstatFile.write((i+1)+"\t"+(stat_x[i]+1)+"\n");
					ystatFile.write((i+1)+"\t"+(stat_y[i]+1)+"\n");
				}
			}
			fw.close();
			xstatFile.close();
			ystatFile.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return fileNamePrefix+".csv";
	}
	
	public static void main(String []args){
		XZipfDistribution distribution = new XZipfDistribution();
		distribution.generate(10000000,0,100,"");
	}
}
