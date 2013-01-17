package com.util.raster;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.geom.Rectangle2D.Double;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
/**
 * This is a raster of the space, the stripe of row is the minimum size of subspace, 
 * The number of columns is defined as a constant number(see 100), 
 * and the version dimension will store the multiple objects which are located into the same box(row and column)
 * @author dan
 *
 */
public class XRaster {

    // min width of subspace;
    private double min_size_of_height = 1;
    /**
     * The number of columns defined by users, this can be caculated with the space scope and density of data.
     */
    private double min_size_of_width = 1;
        
    
    private Rectangle2D.Double m_rect = null;            // The area this QuadTree represents
    private ArrayList<XBox> m_boxes = null;
    private XBox[][] m_matrix = null;
    private DecimalFormat IndexKeyFormatter = null;
    private DecimalFormat IndexColumnFormatter = null;
    private Point2D.Double offset = null;
    
    public XRaster(Rectangle2D.Double rect, double min_size_of_height,Point2D.Double offsetPoint){
    	
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
    	
    	int num_of_row = (int) (this.m_rect.getHeight() / this.min_size_of_height);
    	
    	
    	this.m_boxes = new ArrayList<XBox>();
    	// back up of m_boxes     	
    	this.m_matrix = new XBox[num_of_row+1][num_of_row+1];
    	
    	IndexKeyFormatter = this.getKeyFormatter(num_of_row);   
    	IndexColumnFormatter = this.getKeyFormatter((int)(m_rect.getWidth() / min_size_of_width));
    }
    
    
    /**
     * This is because the space is normalized, so the point should be normalized as well.
     * @return
     */
    private double[] normalize(double x, double y){
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
    /**
     * To Determine the subspace which the point belongs to. The point here is for the original point.
     * It is used to get the point's index
     * @param x
     * @param y
     * @return Box, it indicates the row and column this point belongs to and the number of objects the box has already
     */  
    public XBox locate(double x, double y) {
    	double[] normalized = this.normalize(x, y);  
    	//System.out.println("normalized: "+normalized[0]+";"+normalized[1]);
    	XBox box = this.intervalLocate(normalized[0], normalized[1]);
    	return box;
    }
    /**
     * This is for locating the index for the point within the normailzed space 
     * The range is [), it is consistent with the scan, the stop is also the semi-interval
     * @param x
     * @param y
     * @return
     */
    private XBox intervalLocate(double x, double y){
    	
    	if(x>this.m_rect.getMaxX() || x<this.m_rect.getMinX() || y>this.m_rect.getMaxY() || y<this.m_rect.getMinY())
    		return new XBox(null,null);
    	//System.out.println("interval locate: "+this.m_rect.getX() + ";"+this.m_rect.getY()+";"+this.m_rect.getWidth() + ";"+this.m_rect.getHeight());
    	
    	int row = (int) ((y-this.m_rect.getY()) / this.min_size_of_height );   	
    	int column = (int)((x - this.m_rect.getX()) / this.min_size_of_width );  		
    		
    	String f_row = this.IndexKeyFormatter.format(row);
    	String f_column = this.IndexColumnFormatter.format(column);
/*    	for(XBox box:m_boxes){
    		if(box.getRow().equals(f_row) && box.getColumn().equals(f_column)){    			
    			return box;
    		}
    	} */
    	    	
    	XBox box = new XBox(f_row,f_column);
    	if(this.m_matrix[row][column] == null)
    		this.m_matrix[row][column] = box;
    	else
    		this.m_matrix[row][column].addObject();
    	
    	return box;    	
    }
    /**
     * This is used during uploading, the point here is the original point.
     * @param x
     * @param y
     * @return
     */
    public XBox addPoint(double x, double y){
    	XBox box = this.locate(x, y);
    	//box.addObject();
    	//this.m_boxes.add(box);
    	return box;
    }
    
    public XBox[] match(double x, double y,double radius){    
    	double[] normalized = this.normalize(x, y);
    	x = normalized[0];
    	y = normalized[1];
    	
    	double minX = (m_rect.getMinX()<(x-radius))? (x-radius):m_rect.getMinX(); 
    	double minY = (m_rect.getMinY()<(y-radius))? (y-radius):m_rect.getMinY(); 
    	double maxX = (m_rect.getMaxX()>(x+radius))? (x+radius):m_rect.getMaxX();
    	double maxY = (m_rect.getMaxY()>(y+radius))? (y+radius):m_rect.getMaxY();
    	System.out.println("match bounder: ("+minX+","+minY+")("+maxX+","+maxY+")");
    	
    	XBox tl = this.intervalLocate(minX, minY);    	
    	XBox br = this.intervalLocate(maxX, maxY);    	
    	return new XBox[]{tl,br};    	   	
    }
    
    public XBox[] intersect(Rectangle2D.Double rect)throws Exception{

    	// normalize the rect to locate it into the same coordinate with the grid
    	double[] normalized = this.normalize(rect.getX(),rect.getY());
    	Rectangle2D.Double vRect = new Rectangle2D.Double(normalized[0],normalized[1],
    			rect.getWidth(),rect.getHeight());
    	
/*    	System.out.println("Grid: "+this.m_rect.getX()+";"+this.m_rect.getY()+"==>"+this.m_rect.getWidth()+";"+this.m_rect.getHeight());
    	System.out.println("Range: "+rect.getX()+";"+rect.getY()+"==>"+rect.getWidth()+";"+rect.getHeight());
    	System.out.println("Range: "+vRect.getX()+";"+vRect.getY()+"==>"+vRect.getWidth()+";"+vRect.getHeight());
    	System.out.println("Range: "+vRect.getMinX()+";"+vRect.getMinY()+"==>"+vRect.getMaxX()+";"+vRect.getMaxY());
 */   	
    	Rectangle2D.Double dest = new Rectangle2D.Double();
    	Rectangle2D.Double.intersect(vRect, this.m_rect, dest);
/*    	System.out.println("intersection rectangle: "+dest.getMinX()+";"+dest.getMinY()+"==>"+dest.getMaxX()+";"+dest.getMaxY());
    	System.out.println("intersection rectangle: "+dest.getX()+";"+dest.getY()+"==>"+dest.getWidth()+";"+dest.getHeight());
 */   	
    	XBox tl = this.intervalLocate(dest.getMinX(), dest.getMinY());    	
    	XBox br = this.intervalLocate(dest.getMaxX(), dest.getMaxY());    	
    	
    	return new XBox[]{tl,br};      	    	
    }
    
    public String[] getColumns(XBox top_left, XBox bottom_right){
    	List<String> columns = new ArrayList<String>();
    	System.out.println(top_left.toString());
    	System.out.println(bottom_right.toString());
    	for(int i=Integer.valueOf(top_left.getColumn()); i<= Integer.valueOf(bottom_right.getColumn()); i++){
    		String c = this.IndexColumnFormatter.format(i);
    		//System.out.println("column: "+c);
    		columns.add(c);
    	}
		String[] c = new String[columns.size()];

		c = columns.toArray(c);
		return c;
    }
    
	private DecimalFormat getKeyFormatter(int num_of_key){
		DecimalFormat xIndexFormatter = null;
		if(num_of_key<10){
			xIndexFormatter = new DecimalFormat("0");
		}else if(num_of_key<100){
			xIndexFormatter = new DecimalFormat("00");
		}else if(num_of_key<1000){
			xIndexFormatter = new DecimalFormat("000");
		}else if(num_of_key<10000){
			xIndexFormatter = new DecimalFormat("0000");
		}else if(num_of_key<100000){
			xIndexFormatter = new DecimalFormat("00000");
		}else if(num_of_key<1000000){
			xIndexFormatter = new DecimalFormat("000000");
		}else if(num_of_key<10000000){
			xIndexFormatter = new DecimalFormat("0000000");
		}else if(num_of_key<100000000){
			xIndexFormatter = new DecimalFormat("00000000");
		}else{
			xIndexFormatter = new DecimalFormat("000000000");
		}
		return xIndexFormatter;
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
    	for(XBox box:m_boxes){
    		//msg += box.toString()+"\n";
    	}
    	System.out.println(msg);				
	}
}
