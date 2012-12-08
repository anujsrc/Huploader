package com.query.experiment;

import java.awt.geom.Point2D;

public class TestExp {

	
	public static void main(String[] args){
		Point2D.Double resPoint = new Point2D.Double(0.73,73.0);
		Point2D.Double point2 = new Point2D.Double(45.5,20.5);
		System.out.println(resPoint.distance(point2));
	}
}
