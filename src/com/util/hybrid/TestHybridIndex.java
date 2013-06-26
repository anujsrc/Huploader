package com.util.hybrid;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Hashtable;
import java.util.List;

import com.util.XConstants;
import com.util.raster.X2DGrid;
import com.util.raster.XBox;

public class TestHybridIndex {

    public static void main(String args[]){
    	Point2D.Double offset = new Point2D.Double(138.95,0);
    	Rectangle2D.Double rect = new Rectangle2D.Double(
    			-138.95,41.77,60,19);
   	
    	XHybridIndex hybrid = new XHybridIndex(rect,-1,offset,0.01);
    	hybrid.buildZone(XConstants.ENCODING_DECIMAL);
    	
    	hybrid.printZone();
    	String[] first = hybrid.locate(-133.45518181579706,53.71493542536064);
    	System.out.println(first[0]+";"+first[1]);
    	       
  	
/*    	String[] second = hybrid.deprecatedLocate(7.9,6);
    	System.out.println(second[0]+";"+second[1]);*/
    	
    	
       	Hashtable<String,XBox[]> uResult = hybrid.match(-134.01027800857088,52.60728293702741,1);
       	for(String index: uResult.keySet()){
       		System.out.println(index+"=>"+uResult.get(index)[0].toString()+";"+uResult.get(index)[1].toString());	
       	} 	
    	
/*       	Hashtable<String,XBox[]> result = hybrid.deperatedMatch(8,6,1);
       	for(String index: result.keySet()){
       		System.out.println(index+"=>"+result.get(index)[0].toString()+";"+result.get(index)[1].toString());	
       	}*/
    	
       	
       	
/*    	System.out.println(rect.getCenterX()+","+rect.getCenterY());
    	System.out.println(rect.getMinX()+","+rect.getMinY()+","+rect.getMaxX()+","+rect.getMaxY());
    	System.out.println(rect.getX()+";"+rect.getY());*/
    							
    }
	
	
}
