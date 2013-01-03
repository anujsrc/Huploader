package com.benchmark.dataset;

import java.io.FileWriter;

import org.apache.commons.math3.random.RandomDataImpl;

public class XRandomDistribution {

	
public void generate(int number,int min, int max){	
		
		 
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

		RandomDataImpl randomData2 = new RandomDataImpl();				
		for(int i=0;i<number;i++){
			int value = randomData2.nextZipf(number,1);
			System.out.println(value);
			stat_x[value]++;
		}

		FileWriter xstatFile = null;
		
		try{
		
			xstatFile = new FileWriter("random.spc");
			
			xstatFile.write("m\tVm\n");			
			for(int i=0;i<number;i++){
				if(i<number){
					xstatFile.write((i+1)+"\t"+(stat_x[i])+"\n");					
				}
			}			
			xstatFile.close();			
			
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public static void main(String []args){
		XRandomDistribution distribution = new XRandomDistribution();
		distribution.generate(100,0,100);
	}
	
	
}
