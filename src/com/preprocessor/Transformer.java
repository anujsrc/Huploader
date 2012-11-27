package com.preprocessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Hashtable;

import com.preprocessor.bixi.BixiTransformer;
import com.preprocessor.cosmo.CosmoTransformer;
import com.util.XConstants;


/**
 * Get the correct transform function depending on the different file format input
 * @author dan
 *
 */
public abstract class Transformer {
	
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
			
			transformer = new BixiTransformer();
			transformer.execute(inDir, ".xml", template, outDir);
			
		}else if(format == XConstants.BINARY_FORMAT){
			
			transformer = new CosmoTransformer();
			transformer.execute(inDir, ".bin", template, outDir);
		}else{
			System.out.println("Bad File Format");
		}
		
		
	}
	
	
	protected abstract void parse(String inFile,String outFile);
	
	// This represents the attributes in the raw file
	protected Hashtable<String, String> item_set = null;
	
	public Transformer(){
		item_set = new Hashtable<String,String>();
	}
	
	/**
	 * 
	 * @param inputDir
	 * @param format
	 * @param template
	 * @param outDir
	 */
	public void execute(String inputDir, String fileSuffix, String template, String outDir){
		
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
					if(name.contains(fileSuffix)){
						this.parse(in_directory.getAbsolutePath()+"/"+name,out_directory.getAbsolutePath()+"/"+name+".csv");	
					}									
				}
			}	
		}catch(Exception e){
			e.printStackTrace();
		}				
	}		
	
	
	private Hashtable<String, String>  getTemplate(String template_file){
		
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
		return item_set;
	}	
	
	
	
	
}
