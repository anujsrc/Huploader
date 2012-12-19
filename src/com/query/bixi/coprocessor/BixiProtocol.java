package com.query.bixi.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import com.util.XCSVFormat;

/**
* Provides Bixi specific utilities served as a Coprocessor.
*/
public interface BixiProtocol extends CoprocessorProtocol {
  
  /*******************For location Schema1**QuadTree************************/
  RCopResult copQueryNeighbor4QT(Scan scan,double latitude,double longitude,double radius,XCSVFormat csv)throws IOException;
  RCopResult copQueryPoint4QT(Scan scan,double latitude,double longitude,XCSVFormat csv)throws IOException;
  
  /*******************For location Schema2**************************/
  RCopResult copQueryNeighbor4Raster(Scan scan,double latitude,double longitude,double radius,XCSVFormat csv)throws IOException;
  RCopResult copQueryPoint4Raster(Scan scan,double latitude,double longitude,XCSVFormat csv)throws IOException;
  
  
}