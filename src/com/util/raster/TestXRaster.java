package com.util.raster;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;


public class TestXRaster {

	
    public static void main(String args[]){
    	
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
}
