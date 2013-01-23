package com.query.bixi;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import com.query.QueryAbstraction;
import com.query.bixi.coprocessor.BixiProtocol;
import com.query.bixi.coprocessor.RCopResult;
import com.util.XCSVFormat;
import com.util.XTableSchema;
import com.util.raster.XBox;

public class BixiQuery4Raster extends QueryAbstraction {

	public BixiQuery4Raster(String schema_desc_file, String csv_desc_file,
			String hbase_conf) {
		super(schema_desc_file, csv_desc_file, hbase_conf);
	}

	public BixiQuery4Raster(XTableSchema schema, String csv_desc_file,
			String hbase_conf) {
		super(schema, csv_desc_file, hbase_conf);
	}

	@Override
	public List<String> copQueryAvailableNear(String timestamp,
			final double latitude, final double longitude, final double radius) {
		try {
			this.getCSVLog(0);
			this.getCSVLog(1);
			this.timePhase.clear();
			/** Step1** Call back class definition **/
			class BixiCallBack implements Batch.Callback<RCopResult> {
				RCopResult res = new RCopResult();
				int count = 0;
				QueryAbstraction query = null;

				public BixiCallBack(QueryAbstraction query) {
					this.query = query;
				}

				@Override
				public void update(byte[] region, byte[] row, RCopResult result) {
					long current = System.currentTimeMillis();
					count++;
					res.getRes().addAll(result.getRes()); // to verify the error
															// when large data
					res.setStart(result.getStart());
					res.setEnd(result.getEnd());
					res.setRows((res.getRows() + result.getRows()));
					res.setCells(res.getCells() + result.getCells());
					// write them into csv file
					String outStr = "";
					outStr += "within,"
							+ "cop,"
							+ result.getParameter()
							+ ","
							+ result.getStart()
							+ ","
							+ result.getEnd()
							+ ","
							+ current
							+ ","
							+ result.getRows()
							+ ","
							+ result.getCells()
							+ ","
							+ result.getKvLength()
							+ ","
							+ result.getRes().size()
							+ ","
							+ this.query.regionAndRS
									.get(Bytes.toString(region)) + ","
							+ Bytes.toString(region);
					this.query.writeCSVLog(outStr, 1);
				}
			}
			BixiCallBack callBack = new BixiCallBack(this);

			/** Step2** generate the scan ***********/
			long s_time = System.currentTimeMillis();
			this.timePhase.add(s_time);

			// match the query area in Raster to get the row range and column
			// range
			long match_s = System.currentTimeMillis();
			XBox[] match_boxes = raster.match(latitude, longitude, radius);
			long match_time = System.currentTimeMillis() - match_s;
			String[] rowRange = new String[2];
			rowRange[0] = match_boxes[0].getRow();
			rowRange[1] = match_boxes[1].getRow() + "-*";

			String[] c = raster.getColumns(match_boxes[0], match_boxes[1]);
			// generate the scan
			final Scan scan = hbase.generateScan(rowRange, null,
					new String[] { this.tableSchema.getFamilyName() }, c,
					this.tableSchema.getMaxVersions());

			/**
			 * Step3** send out the query to trigger the corresponding function
			 * in Coprocessor
			 ****/
			this.timePhase.add(System.currentTimeMillis());
			final XCSVFormat csv = this.csvFormat;
			hbase.getHTable().coprocessorExec(BixiProtocol.class,
					scan.getStartRow(), scan.getStopRow(),
					new Batch.Call<BixiProtocol, RCopResult>() {

						public RCopResult call(BixiProtocol instance)
								throws IOException {
							return instance.copQueryNeighbor4Raster(scan,
									latitude, longitude, radius, csv);

						};
					}, callBack);

			long e_time = System.currentTimeMillis();
			this.timePhase.add(e_time);

			long exe_time = e_time - s_time;
			// write to csv file
			String outStr = "";
			outStr += "within," + "cop," + callBack.res.getRes().size() + ","
					+ callBack.res.getCells() + "," + callBack.res.getRows()
					+ "," + exe_time + "," + match_time + ","
					+ this.tableSchema.getSubSpace() + "," + radius;

			for (int i = 0; i < this.timePhase.size(); i++) {
				outStr += ",";
				outStr += this.timePhase.get(i);
			}
			
			outStr +=","+rowRange[0];
			outStr += ","+rowRange[1];
			
			this.writeCSVLog(outStr, 0);

			return callBack.res.getRes();

		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable ee) {
			ee.printStackTrace();
		} finally {
			hbase.closeTableHandler();
			this.closeCSVLog();
		}
		return null;

	}

	@Override
	public HashMap<String, String> scanQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		// return result
		HashMap<String, String> results = new HashMap<String, String>();
		try {

			this.getCSVLog(0);
			this.timePhase.clear();
			long sTime = System.currentTimeMillis();
			this.timePhase.add(sTime);

			Point2D.Double point = new Point2D.Double(latitude, longitude);
			ResultScanner rScanner = null;

			// match rect to find the subspace it belongs to
			long match_s = System.currentTimeMillis();
			XBox[] match_boxes = raster.match(latitude, longitude, radius);
			long match_time = System.currentTimeMillis() - match_s;
			String[] rowRange = new String[2];
			rowRange[0] = match_boxes[0].getRow();
			rowRange[1] = match_boxes[1].getRow() + "-*";

			String[] c = raster.getColumns(match_boxes[0], match_boxes[1]);

			// the version here is harded coded, because i cannot get how many
			// objects in one cell now
			this.timePhase.add(System.currentTimeMillis());
			rScanner = this.hbase.getResultSet(rowRange, null,
					new String[] { this.tableSchema.getFamilyName() }, c,
					this.tableSchema.getMaxVersions());

			int count = 0;
			int row = 0;
			int accepted = 0;

			for (Result r : rScanner) {

				NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = r
						.getMap();

				for (byte[] family : resultMap.keySet()) {
					NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = resultMap
							.get(family);
					for (byte[] col : columns.keySet()) {
						NavigableMap<Long, byte[]> values = columns.get(col);
						for (Long version : values.keySet()) {
							count++;
							// get the distance between this point and the given
							// point

							Hashtable<String, String> key_values = this.csvFormat
									.fromPairString(Bytes.toString(values
											.get(version)));

							Point2D.Double resPoint = new Point2D.Double(Double
									.valueOf(key_values.get("lat"))
									.doubleValue(), Double.valueOf(
									key_values.get("long")).doubleValue());
							double distance = resPoint.distance(point);

							if (distance <= radius) {
								// System.out.println("row=>"+Bytes.toString(r.getRow())
								// +
								// ";colum=>"+Bytes.toString(col)+";station=>"+key_values.get("id")+";distance=>"+distance+";latitude=>"+key_values.get("lat")+";long=>"+key_values.get("long"));
								accepted++;
								results.put(key_values.get("id"),
										String.valueOf(distance));
							}
						}
					}
				}
			}
			long eTime = System.currentTimeMillis();
			this.timePhase.add(eTime);

			// write to csv file
			String outStr = "";
			outStr += "within," + "scan," + accepted + "," + count + "," + row
					+ "," + (eTime - sTime) + "," + match_time + ","
					+ this.tableSchema.getSubSpace() + "," + radius;

			for (int i = 0; i < this.timePhase.size(); i++) {
				outStr += ",";
				outStr += this.timePhase.get(i);
			}
			outStr +=","+rowRange[0];
			outStr += ","+rowRange[1];
			this.writeCSVLog(outStr, 0);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.hbase.closeTableHandler();
			this.closeCSVLog();
		}
		return results;
	}


	@Override
	public HashMap<String,Double> copQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		
		try {
			// create the log file
			this.getCSVLog(0);
			this.getCSVLog(1);
			//
			this.timePhase.clear();
			/** Step1** Call back class definition **/

			class BixiCallBack implements Batch.Callback<RCopResult> {
				RCopResult res = new RCopResult();
				int count = 0; // the number of coprocessor
				QueryAbstraction query = null;

				public BixiCallBack(QueryAbstraction query) {
					this.query = query;
				}

				@Override
				public void update(byte[] region, byte[] row, RCopResult result) {
					long current = System.currentTimeMillis();
					count++;
					res.getDistances().putAll(result.getDistances());																	
					res.setStart(result.getStart());
					res.setEnd(result.getEnd());
					res.setRows((res.getRows() + result.getRows()));
					res.setCells(res.getCells() + result.getCells());
					// write them into csv file
					String outStr = "";
					outStr += "within,"
							+ "cop,"
							+ result.getParameter()
							+ ","
							+ result.getStart()
							+ ","
							+ result.getEnd()
							+ ","
							+ current
							+ ","
							+ result.getRows()
							+ ","
							+ result.getCells()
							+ ","
							+ result.getKvLength()
							+ ","
							+ result.getRes().size()
							+ ","
							+ this.query.regionAndRS
									.get(Bytes.toString(region)) + ","
							+ Bytes.toString(region);
					this.query.writeCSVLog(outStr, 1);

				}
			}

			BixiCallBack callBack = new BixiCallBack(this);

			/** Step2*** generate scan ***/
			// build up a quadtree.
			long s_time = System.currentTimeMillis();
			this.timePhase.add(s_time);
			// match rect to find the subspace it belongs to

			// Step1: estimate the window circle for the first time
			long total_points = this.tableSchema.getTotalNumberOfPoints();
			double areaOfMBB = this.tableSchema.getEntireSpace().width
					* this.tableSchema.getEntireSpace().height;
			double DensityOfMBB = total_points / areaOfMBB;
			double init_radius = Math.sqrt(n / DensityOfMBB);

			final double x = Math.abs(latitude);
			final double y = Math.abs(longitude);
			int count = 0;

			// Step2: trigger a scan to get the points based on the above window
			int iteration = 1;
			double radius = (init_radius > this.tableSchema.getSubSpace()) ? init_radius
					: this.tableSchema.getSubSpace();			
			this.timePhase.add(System.currentTimeMillis());

			do {
				
				String str = "iteration" + iteration + "; count=>" + count
						+ ";radius=>" + radius;
				System.out.println(str);
				// match rect to find the subspace it belongs to
				XBox[] match_boxes = this.raster.match(latitude, longitude,
						radius);

				String[] rowRange = new String[2];
				rowRange[0] = match_boxes[0].getRow();
				rowRange[1] = match_boxes[1].getRow() + "-*";

				String[] c = raster.getColumns(match_boxes[0], match_boxes[1]);
				// generate the scan
				final Scan scan = hbase.generateScan(rowRange, null,
						new String[] { this.tableSchema.getFamilyName() }, c,
						this.tableSchema.getMaxVersions());
				
				System.out.println("start to send the query to coprocessor.....");

				/** Step3: send request to trigger Coprocessor execution **/

				final XCSVFormat csv = this.csvFormat;
				final double estimated_radius = radius;
				hbase.getHTable().coprocessorExec(BixiProtocol.class,
						scan.getStartRow(), scan.getStopRow(),
						new Batch.Call<BixiProtocol, RCopResult>() {

							public RCopResult call(BixiProtocol instance)
									throws IOException {
								//TODO should change the function name
								return instance.copQueryNeighbor4Raster(scan,
										x, y, estimated_radius, csv);

							};
						}, callBack);
			
				radius = radius*(1+ (n*1.0/count));//init_radius * (iteration + 1);

			} while (count < n && (++iteration > 0));			

			long cop_end = System.currentTimeMillis();
			this.timePhase.add(cop_end);		
			
			long exe_time = cop_end - s_time;

			//ArrayList<Double> tempArray = new ArrayList<Double>(
			java.lang.Double[] tempArray = (java.lang.Double[])callBack.res.getDistances().values().toArray();			
			Collections.sort(Arrays.asList(tempArray));
			
			// write to csv file
			String outStr = "";
			outStr += "within," + "cop," + callBack.res.getRes().size() + ","
					+ callBack.res.getCells() + "," + callBack.res.getRows()
					+ "," + exe_time + "," + "-1" + ","
					+ this.tableSchema.getSubSpace() + "," + radius;

			for (int i = 0; i < this.timePhase.size(); i++) {
				outStr += ",";
				outStr += this.timePhase.get(i);
			}
			outStr += "," + tempArray[0] + "," + tempArray[tempArray.length-1];
			this.writeCSVLog(outStr, 0);	
			
			
			return callBack.res.getDistances();

		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable ee) {
			ee.printStackTrace();
		} finally {
			hbase.closeTableHandler();
			this.closeCSVLog();
		}

		return null;				
		
		
		
		
	}

	@Override
	public TreeMap<Double, String> scanQueryAvailableKNN(String timestamp,
			double latitude, double longitude, int n) {

		TreeMap<Double, String> sorted = null;
		try {
			this.getCSVLog(0);
			long sTime = System.currentTimeMillis();
			this.timePhase.clear();
			this.timePhase.add(System.currentTimeMillis());

			// Step1: estimate the window circle for the first time
			long total_points = this.tableSchema.getTotalNumberOfPoints();
			double areaOfMBB = this.tableSchema.getEntireSpace().width
					* this.tableSchema.getEntireSpace().height;
			double DensityOfMBB = total_points / areaOfMBB;
			double init_radius = Math.sqrt(n / DensityOfMBB);					

			longitude = Math.abs(longitude);
			int count = 0;
			ResultScanner rScanner = null;
			List<String> resultsList = new ArrayList<String>();

			// Step2: trigger a scan to get the points based on the above window
			int iteration = 1;

			long match_s = System.currentTimeMillis();
			double radius = (init_radius > this.tableSchema.getSubSpace()) ? init_radius
					: this.tableSchema.getSubSpace();
			
			this.timePhase.add(System.currentTimeMillis());
			
			do {
				String str = "iteration" + iteration + "; count=>" + count
						+ ";radius=>" + radius;
				System.out.println(str);
				// match rect to find the subspace it belongs to
				XBox[] match_boxes = this.raster.match(latitude, longitude,
						radius);

				String[] rowRange = new String[2];
				rowRange[0] = match_boxes[0].getRow();
				rowRange[1] = match_boxes[1].getRow() + "-*";

				String[] c = raster.getColumns(match_boxes[0], match_boxes[1]);
				rScanner = this.hbase.getResultSet(rowRange, null,
						new String[] { this.tableSchema.getFamilyName() }, c,
						this.tableSchema.getMaxVersions());

				count = 0;
				resultsList.clear();
				for (Result r : rScanner) {
					NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = r
							.getMap();

					for (byte[] family : resultMap.keySet()) {
						NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = resultMap
								.get(family);
						for (byte[] col : columns.keySet()) {
							NavigableMap<Long, byte[]> values = columns
									.get(col);
							for (Long version : values.keySet()) {
								resultsList.add(Bytes.toString(values
										.get(version)));
								count++;
							}
						}
					}
				}
				// Step3: get the result,estimate the window circle next
				// depending on the previous step result, util we got the K
				// nodes
				radius = init_radius * (iteration + 1);

			} while (count < n && (++iteration > 0));
			String str = "iteration" + iteration + "; count=>" + count
					+ ";radius=>" + radius;
			System.out.println(str);

			long match_time = System.currentTimeMillis() - match_s;

			// Step4: get all possible points and sort them by the distance and
			// get the top K
			Point2D.Double point = new Point2D.Double(latitude, longitude);
			// result container
			HashMap<Double, String> distanceMap = new HashMap<Double, String>();

			for (String value : resultsList) {
				Hashtable<String, String> key_values = this.csvFormat
						.fromPairString(value);

				Point2D.Double resPoint = new Point2D.Double(Double.valueOf(
						key_values.get("lat")).doubleValue(), Double.valueOf(
						key_values.get("long")).doubleValue());

				double distance = resPoint.distance(point);

				distanceMap.put(distance, key_values.get("id"));
			}

			long eTime = System.currentTimeMillis();
			this.timePhase.add(System.currentTimeMillis());
			sorted = new TreeMap<Double, String>(distanceMap);

			System.out.println("all values: " + sorted.values().toString());
			// write to csv file
			String outStr = "";
			outStr += "knn," + "scan," + sorted.size() + "," + count + ","
					+ iteration + "," + (eTime - sTime) + "," + match_time
					+ "," + this.tableSchema.getSubSpace() + "," + n ;

			for (int i = 0; i < this.timePhase.size(); i++) {
				outStr += ",";
				outStr += this.timePhase.get(i);
			}
			// additional metrics
			outStr += ","+ sorted.firstKey() + "," + sorted.lastKey();
			
			this.writeCSVLog(outStr, 0);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.hbase.closeTableHandler();
			this.closeCSVLog();
		}
		return sorted;
	}

	@Override
	public String copQueryPoint(final double latitude, final double longitude) {

		try {
			this.getCSVLog(0);
			this.getCSVLog(2);
			this.timePhase.clear();
			/** Step1** Call back class definition **/
			class BixiCallBack implements Batch.Callback<RCopResult> {
				RCopResult res = new RCopResult();
				QueryAbstraction query = null;

				public BixiCallBack(QueryAbstraction query) {
					this.query = query;
				}

				@Override
				public void update(byte[] region, byte[] row, RCopResult result) {
					long current = System.currentTimeMillis();
					if (result.getRes() != null)
						res.getRes().addAll(result.getRes()); // to verify the
																// error when
																// large data
					res.setStart(result.getStart());
					res.setEnd(result.getEnd());
					res.setRows((res.getRows() + result.getRows()));
					res.setCells(res.getCells() + result.getCells());
					// write them into csv file
					String outStr = "";
					outStr += "within,"
							+ "cop,"
							+ result.getParameter()
							+ ","
							+ result.getStart()
							+ ","
							+ result.getEnd()
							+ ","
							+ current
							+ ","
							+ result.getRows()
							+ ","
							+ result.getCells()
							+ ","
							+ result.getKvLength()
							+ ","
							+ result.getRes().size()
							+ ","
							+ this.query.regionAndRS
									.get(Bytes.toString(region)) + ","
							+ Bytes.toString(region);
					this.query.writeCSVLog(outStr, 1);
				}

			}
			BixiCallBack callBack = new BixiCallBack(this);

			/** Step2*** generate scan ***/
			// record the start time
			long s_time = System.currentTimeMillis();
			this.timePhase.add(s_time);

			// match rect to find the subspace it belongs to
			long match_s = System.currentTimeMillis();
			XBox match_box = raster.locate(latitude, longitude);
			long match_time = System.currentTimeMillis() - match_s;
			String[] rowRange = new String[2];
			rowRange[0] = match_box.getRow();
			rowRange[1] = match_box.getRow() + "0";

			// generate the scan
			final Scan scan = hbase.generateScan(rowRange, null,
					new String[] { this.tableSchema.getFamilyName() },
					new String[] { match_box.getColumn() },
					this.tableSchema.getMaxVersions());

			System.out.println("start to send the query to coprocessor.....");

			/** Step3: send request to trigger Coprocessor execution **/
			this.timePhase.add(System.currentTimeMillis());
			final XCSVFormat csv = this.csvFormat;
			hbase.getHTable().coprocessorExec(BixiProtocol.class,
					scan.getStartRow(), scan.getStopRow(),
					new Batch.Call<BixiProtocol, RCopResult>() {

						public RCopResult call(BixiProtocol instance)
								throws IOException {

							return instance.copQueryPoint4QT(scan, latitude,
									longitude, csv);

						};
					}, callBack);

			long e_time = System.currentTimeMillis();
			this.timePhase.add(e_time);

			long exe_time = e_time - s_time;
			// write to csv file
			String outStr = "";
			outStr += "within," + "cop," + callBack.res.getRes().size() + ","
					+ callBack.res.getCells() + "," + callBack.res.getRows()
					+ "," + exe_time + "," + match_time + ","
					+ this.tableSchema.getSubSpace() + ",(" + latitude + ":"
					+ longitude + ")";

			for (int i = 0; i < this.timePhase.size(); i++) {
				outStr += ",";
				outStr += this.timePhase.get(i);
			}
			this.writeCSVLog(outStr, 0);
			if (callBack.res.getRes().size() > 0) {
				callBack.res.getRes().get(0);
			}

			System.out.println("The point " + "(" + latitude + ":" + longitude
					+ ") does not be found ");

		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable ee) {
			ee.printStackTrace();
		} finally {
			hbase.closeTableHandler();
			this.closeCSVLog();
		}

		return null;

	}

	@Override
	public String scanQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	public  List<String> copQueryAvailableNearOnFilter(String timestamp,
			double latitude, double longitude, final double radius){
		return null;
	}
}
