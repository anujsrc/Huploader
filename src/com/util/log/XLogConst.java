package com.util.log;

public class XLogConst {

	public static String[] main_header = new String[]{
			"query",
			"method",
			"result",
			"scannedCell",
			"scannedRow",
			"exeTime",			
			"matchTime",
			"subspace",
			"param",
			"StartTS",
			"beforeScanTS",
			"EndTS"			
	};
	
	
	public static String[] cop_header = new String[]{
		"query",
		"method",
		"param",
		"copStart",
		"copEnd",
		"reachClient",		
		"scannedRow",
		"scannedCell",
		"kvLength",
		"accpted",
		"RS",
		"region"				
	};
	
	public static String[] time_header = new String[] {
		"query",
		"method",		
		"param",
		"time1",
		"time2",
		"time3",
		"time4",
		"total"
		
	};
	
	
	
}
