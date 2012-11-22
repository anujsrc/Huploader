package com.preprocessor;

import com.util.XConstants;


/**
 * Get the correct transform function depending on the different file format input
 * @author dan
 *
 */
public class Transformer {

	/**
	 * 
	 * @param inputDir
	 * @param format
	 * @param template
	 * @param outDir
	 */
	public void execute(String inputDir, int format, String template, String outDir){
		return;
	}
	
	
	// input: input directory, file format, template file, output directory
	// output: the file is transformed to csv files 
	public static void main(String[] args){
		
		if(args.length < 4)
			System.out.println("the argements should be input directory, file format, template file, output directory");
		
		String inDir = args[0];
		int format = Integer.valueOf(args[1]);
		String template = args[2];
		String outDir = args[3];
		
		Transformer transformer = null;
		
		if(format == XConstants.XML_FORMAT){
			
			transformer = new XMLTransformer();
			transformer.execute(inDir, format, template, outDir);
			
		}else if(format == XConstants.BINARY_FORMAT){
			
			transformer = new BinaryTransformer();
			transformer.execute(inDir, format, template, outDir);
		}else{
			System.out.println("Bad File Format");
		}
		
		
	}
}
