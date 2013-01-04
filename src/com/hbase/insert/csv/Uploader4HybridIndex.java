package com.hbase.insert.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;

public class Uploader4HybridIndex extends CSVDataUploader{
	
	
	public Uploader4HybridIndex(String csv_desc_file, String schema_desc_file,String hbaseConf){
		super(csv_desc_file, schema_desc_file, hbaseConf);
	}
	
	@Override
	public void upload(String input_dir, int batchNum) throws Exception {
			
		// read the file,
		File dir = new File(input_dir);
		if (!dir.isDirectory()){
			throw new Exception("there is no this input directory!");			
		}
			

		String[] fileNames = dir.list();

		long start = System.currentTimeMillis();

		BufferedReader in = null;
		int file_num = 0;
		int num_of_row = 0;
				
		String family = this.tableSchema.getFamilyName();
		// get lantitude and longitude index, id index		
		this.locateKeyIndicator();

		ArrayList<Put> putList = new ArrayList<Put>();
	
		for (String fileName : fileNames) {

			if (!fileName.endsWith(".csv"))
				continue;
			long fstart = System.currentTimeMillis();
			int totalRow = 0;
			try {

				in = new BufferedReader(new FileReader(input_dir + "/"
						+ fileName));


				// read the file
				String line = in.readLine().trim();
				
				while (line != null) {
					line = line.trim();
					if (line.length() == 0) {
						continue;
					}
					String[] values = line.split(",");
					// generate cell indicator and cell value	
					//System.out.println(lan_index+";"+long_index+";"+id_index);
					String[] cell_indicator = this.getCellIndicator(values[lan_index],values[long_index],values[id_index]);									
					
					// insert it into hbase
					Put put = new Put(cell_indicator[0].getBytes());
					// add all attribute of one object into multiple cells corresponding to versions
					for(int i=0;i<values.length;i++){
						put.add(family.getBytes(), cell_indicator[1].getBytes(), Long.valueOf(i+1),values[i].getBytes());
					}						
										
					putList.add(put);
					num_of_row++;

					if (putList.size() == batchNum) {
						hbase.flushBufferedRow(putList);
						totalRow += batchNum;
						putList.clear();
					}
					line = in.readLine();
				}

				// for the last lines
				if (putList.size() > 0) {
					hbase.flushBufferedRow(putList);
					totalRow += putList.size();
					putList.clear();
				}
				file_num++;
				in.close();
				System.out.println("file_name=>" + fileName + ";file_num=>"
						+ file_num + ";exe_time=>"
						+ (System.currentTimeMillis() - fstart)
						+ ";file_total_number=>" + totalRow);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null)
						in.close();
				} catch (Exception e) {
					e.printStackTrace();
				}			
			}
		} // end of files list
		
		this.hbase.closeTableHandler();
		System.out.println("file_num=>"+file_num+";exe_time=>"
				+ (System.currentTimeMillis() - start) + ";total_number=>"
				+ num_of_row);				
	}





	@Override
	protected String[] getCellIndicator(String lan, String longitude, String id) {
		if(lan == null || longitude == null)
			return null;
		String indicator[] = new String[3];		
		if(this.hybrid != null){
			
			String[] keys = this.hybrid.locate(Double.valueOf(lan).doubleValue(),Double.valueOf(longitude).doubleValue());
			indicator[0] = keys[0];
			indicator[1] = keys[1]+"-"+id;
			// indicator[2] = "1"; // this does not make sense
		}					
		return indicator;
	}

	
	
	
	

}
