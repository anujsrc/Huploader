package com.maprd.insert;

import java.io.IOException;


import com.maprd.insert.bixi.HBaseInserter4Bixi;
import com.maprd.insert.cosmo.HBaseInserter4Cosmo;
import com.util.XConstants;

/**
 * This is to get the command from client and distribute to each class
 * 
 * @author dan
 * 
 */

public abstract class HBaseInserter {

	protected String schema_desc_file = null;
	protected String input_dir = null;
	protected String input_desc_file = null;

	public HBaseInserter(String input_dir, String input_desc_file,
			String schema_desc_file) {
		this.schema_desc_file = schema_desc_file;
		this.input_desc_file = input_desc_file;
		this.input_dir = input_dir;
	}
	public abstract void execute() throws Exception;
	
	public static void main(String[] args) throws IOException {

		if (args.length < 4) {
			System.out
					.println("<dataset> <input directory in HDFS> <input desc> <table desc>");
		}

		int dataset = Integer.valueOf(args[0]);
		String inputDir = args[1];
		String input_desc = args[2];
		String table_desc = args[3];

		HBaseInserter inserter = null;

		try{
			if (dataset == XConstants.DATA_BIXI) {

				inserter = new HBaseInserter4Bixi(inputDir, input_desc, table_desc);
				inserter.execute();

			} else if (dataset == XConstants.DATA_COSMO) {

				inserter = new HBaseInserter4Cosmo(inputDir, input_desc, table_desc);
				inserter.execute();

			}
	
		}catch(Exception e){
			e.printStackTrace();
		}
		

	}

}
