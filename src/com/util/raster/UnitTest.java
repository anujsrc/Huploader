package com.util.raster;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;


public class UnitTest {

	
	public void testRaster(){
    	Rectangle2D.Double space = new Rectangle2D.Double(-90,-180,180,360);
    	Point2D.Double offset = new Point2D.Double(90,180);
    	XRaster raster = new XRaster(space,	10,offset); 
    	
    	raster.print();
    	
    	
    	// get one point's index
    	XBox box = raster.locate(-89,120);
    	System.out.println("============");
    	System.out.println(box.toString());
    	
    	XBox[] boxes = raster.match(-89,120,100);
    	System.out.println("=============match...");
    	System.out.println(boxes[0].toString());
    	System.out.println(boxes[1].toString());
	}
	
	public static void test3DGrid(){
    	Rectangle2D.Double space = new Rectangle2D.Double(0,0,10,10);
    	Point2D.Double offset = new Point2D.Double(90,180);
    	X3DGrid raster = new X3DGrid(space,	1,null); 
    	
    	raster.print();
    	
    	
    	// get one point's index
    	XBox box = raster.locate(1,1);
    	XBox box2 = raster.locate(1.1,1.1);
    	System.out.println("============");
    	System.out.println("first box: "+box.toString());
    	System.out.println("second box: "+box2.toString());
    	
    	XBox[] boxes = raster.match(2,2,1);
    	System.out.println("=============match...");
    	System.out.println(boxes[0].toString());
    	System.out.println(boxes[1].toString());
	}	
	
	public static void test2DGrid(){
    	Rectangle2D.Double space = new Rectangle2D.Double(-90,-180,180,360);
    	Point2D.Double offset = new Point2D.Double(90,180);
    	X2DGrid raster = new X2DGrid(space,	10,offset); 
    	
    	raster.print();
    	
    	// get one point's index
    	XBox box = raster.locate(-89,120);
    	System.out.println("============");
    	System.out.println(box.toString());
    	
    	XBox[] boxes = raster.match(-89,120,100);
    	System.out.println("=============match...");
    	System.out.println(boxes[0].toString());
    	System.out.println(boxes[1].toString());
	}	
	
    public static void main(String args[]){
    	
    	//UnitTest.test2DGrid();
    	UnitTest.test3DGrid();
    	
    	
    }
}
