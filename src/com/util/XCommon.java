package com.util;

import java.text.DecimalFormat;

public class XCommon {
	
	public static DecimalFormat getKeyFormatter(int num_of_key){
		DecimalFormat xIndexFormatter = null;
		if(num_of_key<10){
			xIndexFormatter = new DecimalFormat("0");
		}else if(num_of_key<100){
			xIndexFormatter = new DecimalFormat("00");
		}else if(num_of_key<1000){
			xIndexFormatter = new DecimalFormat("000");
		}else if(num_of_key<10000){
			xIndexFormatter = new DecimalFormat("0000");
		}else if(num_of_key<100000){
			xIndexFormatter = new DecimalFormat("00000");
		}else if(num_of_key<1000000){
			xIndexFormatter = new DecimalFormat("000000");
		}else if(num_of_key<10000000){
			xIndexFormatter = new DecimalFormat("0000000");
		}else if(num_of_key<100000000){
			xIndexFormatter = new DecimalFormat("00000000");
		}else{
			xIndexFormatter = new DecimalFormat("000000000");
		}
		return xIndexFormatter;
	} 
	
	/**
	 * 
	 * @param formattedString : 001, increase it with 1
	 * @return 002
	 */
	public static String IncFormatString(String formattedStr){		
		String value = String.valueOf(Integer.valueOf(formattedStr).intValue()+1);
		int num_of_zero = formattedStr.length()-value.length();
		for(int i=0;i<num_of_zero;i++){
			value = "0"+value;
		}
		return value;
		
	} 	
}
