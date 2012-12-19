package com.util.quadtree.trie;


import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.List;

import com.util.XConstants;


public class TestXQuadTree {

    public static void main(String args[]){
    	
    	Point2D.Double offset = new Point2D.Double(90,180);
    	Rectangle2D.Double rect = new Rectangle2D.Double(
   			 -90,-180,180,360);
   	
    	XQuadTree tree = new XQuadTree(rect,10,offset);
    	tree.buildTree(XConstants.ENCODING_BINARY);
    	//tree.print();
    	   	
       	List<String> result = tree.match(-89,120,1);
    	System.out.println(result.size()+"match========="+result.toString()); //0100,0110
    	
    	// get one point's index
    	XQuadTree subspace = tree.locate(0.15,0.15);
    	System.out.println("============");
    	subspace.print();
   
    	
    	//=========For Decimal Tree======================//
    	
  /* 	XQuadTree decimalTree = new XQuadTree(rect,10,offset);
    	decimalTree.buildTree(XConstants.ENCODING_DECIMAL);
    	//decimalTree.print();
*/     	
    	
/*    	result.clear();
    	result = decimalTree.match(-31,40,1,1);
    	System.out.println(result.size()+"match========="+result.toString()); //0100,0110
    	// get one point's index
    	subspace = decimalTree.locate(0.15,0.15);
    	System.out.println("============");
    	subspace.print();*/
    	
/*    	Rectangle2D.Double rect = new Rectangle2D.Double(
    			 BixiConstant.MONTREAL_TOP_LEFT_X,
    			 BixiConstant.MONTREAL_TOP_LEFT_Y,
    			 BixiConstant.MONTREAL_AREA_WIDTH,
    			 BixiConstant.MONTREAL_AREA_HEIGHT);
    	
    	XQuadTree tree = new XQuadTree(rect,BixiConstant.MIN_SIZE_OF_SUBSPACE);
    	tree.buildTree();
    	tree.print();
    	
    	// get one point's index
    	XQuadTree subspace = tree.locate(45.52830025,73.608938);
    	System.out.println("============");
    	subspace.print();
    	
    	String[] spaces=tree.match(45.51038,73.55653,0.02,0.02);
    	System.out.println("match========="+spaces.length); //0100,0110
    	for(String s:spaces){
    		if(s!=null)
    			System.out.println(s);
    	}
    	
    	//Test for the query neighbor
       	subspace = tree.locate(45.49520,73.56328);
    	System.out.println("============");
    	subspace.print();*/
    }
}
