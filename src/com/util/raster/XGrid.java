package com.util.raster;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.util.XCommon;

public abstract class XGrid {

	
    // min width of subspace;
    protected double min_size_of_height = 1;
    /**
     * The number of columns defined by users, this can be caculated with the space scope and density of data.
     */
    protected double min_size_of_width = 1;
            
    protected Rectangle2D.Double m_rect = null;            // The area this QuadTree represents        
    protected DecimalFormat IndexKeyFormatter = null;
    protected DecimalFormat IndexColumnFormatter = null;
    protected Point2D.Double offset = null;
    protected int num_of_row = -1;
    
    public XGrid(Rectangle2D.Double rect, double min_size_of_height,Point2D.Double offsetPoint){
    	
    	this.min_size_of_height = min_size_of_height;
    	this.min_size_of_width = min_size_of_height;
    	
    	if(offsetPoint != null){
    		this.offset = new Point2D.Double(offsetPoint.x,offsetPoint.y);  
    		if(rect.x < 0 || rect.y < 0){
    			this.m_rect = new Rectangle2D.Double((rect.x + offsetPoint.x),(rect.y + offsetPoint.y),rect.width,rect.height);	
    		}else{
    			this.m_rect = new Rectangle2D.Double((rect.x - offsetPoint.x),(rect.y - offsetPoint.y),rect.width,rect.height);
    		}
    		
    	}else{       	
        	this.m_rect = new Rectangle2D.Double(rect.x,rect.y,rect.width,rect.height);    	
    	}
    	
    	this.num_of_row = (int) (this.m_rect.getHeight() / this.min_size_of_height);
    	    	
    	IndexKeyFormatter = XCommon.getKeyFormatter(num_of_row);   
    	IndexColumnFormatter = XCommon.getKeyFormatter((int)(m_rect.getWidth() / min_size_of_width));
    }
    
    /**
     * To Determine the subspace which the point belongs to. The point here is for the original point.
     * It is used to get the point's index
     * @param x
     * @param y
     * @return Box, it indicates the row and column this point belongs to and the number of objects the box has already
     */  
    public abstract XBox locate(double x, double y);
   
    
    public abstract XBox[] match(double x, double y,double radius);
    
    
    
    public abstract XBox[] intersect(Rectangle2D.Double rect)throws Exception;
    
    
    /**
     * This is because the space is normalized, so the point should be normalized as well.
     * @return
     */
    protected double[] normalize(double x, double y){
    	double[] result = new double[]{x,y};
    	if(this.offset != null){
    		if(x<0 || y < 0){
    			result[0] += this.offset.x;
        		result[1] += this.offset.y;	
    		}else{
    			result[0] -= this.offset.x;
        		result[1] -= this.offset.y;	    			
    		}
    		
    	}
    	return result; 
    }    

    
 
    

	public void print(){
		String msg = "";
		int num_of_row = (int) (this.m_rect.getHeight() / this.min_size_of_height); 
		int num_of_column = (int) (this.m_rect.getWidth() / this.min_size_of_width);
    	double br_x = this.m_rect.getX()+(num_of_column-1)*this.min_size_of_width;
    	double br_y = this.m_rect.getY()+(num_of_row-1)*this.min_size_of_height;    
    	msg = "row=>"+num_of_row+
    				  ";column=>" + num_of_column+
    				  ";lt_x=>" + this.m_rect.getX()+
    				  ";lt_y=>" + this.m_rect.getY()+
    				  ";rb_x=>"+br_x+
    				  ";rb_y=>"+br_y;
    	System.out.println(msg);				
	}
	
}
