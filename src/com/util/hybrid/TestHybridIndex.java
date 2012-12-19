package com.util.hybrid;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Hashtable;
import java.util.List;

import com.util.XConstants;
import com.util.raster.XBox;

public class TestHybridIndex {

    public static void main(String args[]){
    	Point2D.Double offset = new Point2D.Double(0,0);
    	Rectangle2D.Double rect = new Rectangle2D.Double(
   			 0,0,16,16);
   	
    	XHybridIndex hybrid = new XHybridIndex(rect,4,null,1);
    	hybrid.buildZone(XConstants.ENCODING_DECIMAL);
    	//hybrid.printZone();
    	
       	Hashtable<String,XBox[]> result = hybrid.match(8,6,1);
       	for(String index: result.keySet()){
       		System.out.println(index+"=>"+result.get(index)[0].toString()+";"+result.get(index)[1].toString());	
       	}
    	
    	System.out.println(rect.getCenterX()+","+rect.getCenterY());
    	System.out.println(rect.getMinX()+","+rect.getMinY()+","+rect.getMaxX()+","+rect.getMaxY());
    	System.out.println(rect.getX()+";"+rect.getY());
    							
    }
	
	
}
