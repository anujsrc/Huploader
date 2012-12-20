package com.util.hybrid;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.geom.Rectangle2D.Double;
import java.util.Hashtable;
import java.util.List;

import com.util.quadtree.trie.XQuadTree;
import com.util.raster.XBox;
import com.util.raster.XRaster;

public class XHybridIndex {

	private XQuadTree quadtree = null;
	// the size of tile for the first level of quad tree 
	private double tile_size = 1;
	
	// the height of subspace in raster
    private double cell_size = 1;
	
    public XHybridIndex(Rectangle2D.Double rect,double tile_size,Point2D.Double offsetPoint,
    						double cell_size){
    	
    	this.tile_size = tile_size;
    	this.quadtree = new XQuadTree(rect,tile_size,offsetPoint);    	
    	this.cell_size = cell_size;
    }
    /**
     * build the index in the first level
     * @param coding
     */
    public void buildZone(int coding){
    	this.quadtree.buildTree(coding);
    }
    
    /**
     * get index for the data point, the column index should be appended the location id later
     * row index: (QT tile index - row index, column index)
     * @param x latitude of the location point
     * @param y longitude of the location point
     * @return
     */
    public String[] locate(double x, double y) {    	
    	// filter the tile with quad tree first
    	XQuadTree tile = this.quadtree.locate(x, y);
    	// get index in the first level
    	String tile_index = tile.getIndex();
    	// get the tile rect where the point is located
    	Rectangle2D.Double tile_rect = tile.getM_rect();
    	Point2D.Double offsetPoint = new Point2D.Double(tile_rect.getX(),tile_rect.getY());
    	XRaster grid = new XRaster(tile_rect,this.cell_size,offsetPoint); 
    	XBox box = grid.locate(x, y);
    	String[] indexes = new String[2];
    	indexes[0] = tile_index+"-"+box.getRow();
    	indexes[1] = box.getColumn();   
    	System.out.println("row=> "+indexes[0]+";column=>"+indexes[1]);
    	return indexes;    	
    }

    /**
     * match the input rectangle to support range query
     * the return is 
     * @param x
     * @param y
     * @param radius
     * @return an array of two boxes object, and the first box represents
     * the left top point and the second box represents the right bottom point
     */
    public Hashtable<String,XBox[]> match(double x, double y,double radius){
    	System.out.println("in matching./..."+x+";"+y);
    	Rectangle2D.Double matchRect = new Rectangle2D.Double(x-radius,y-radius,2*radius,2*radius);
    	List<XQuadTree> tiles = this.quadtree.tileMatch(x, y,radius);    	
    	Hashtable<String,XBox[]> result = null;    	
    	try{    	
    		if(tiles != null && tiles.size() > 0){
        		result = new Hashtable<String,XBox[]>();
        		for(int i=0;i<tiles.size();i++){
        			XQuadTree oneTile = tiles.get(i);
        			String tileIndex = oneTile.getIndex();
        			// get the tile rect where the point is located
        	    	Rectangle2D.Double tile_rect = oneTile.getM_rect();
        	    	Point2D.Double offsetPoint = new Point2D.Double(tile_rect.getX(),tile_rect.getY());
        	    	//System.out.println(tile_rect.toString()+"==="+this.cell_size+"==="+offsetPoint.toString());
        	    	XRaster grid = new XRaster(tile_rect,this.cell_size,offsetPoint);     	    	        	    	
        			XBox[] range = grid.intersect(matchRect);
        			result.put(tileIndex, range);        			
        		}        		
        	}    		    		
    		
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	return result;    	
    }
    
    
    /**
     * for debug
     */
    public void printZone(){
    	this.quadtree.print();
    }
    
	
}
