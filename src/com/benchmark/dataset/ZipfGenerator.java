package com.benchmark.dataset;

import java.util.Random;


//http://code.google.com/p/xtreemfs/source/browse/branches/vivaldi_eval/src/servers/src/org/xtreemfs/osd/vivaldi/ZipfGenerator.java?r=1428
public class ZipfGenerator {

    private Random rnd;
    //private Random rnd = new Random(0);
    private int size;
    private double skew;
    private double bottom;

    public ZipfGenerator(int size, double skew) {
        this.rnd = new Random(System.currentTimeMillis());
        this.size = size;
        this.skew = skew;
        //calculate the generalized harmonic number of order 'size' of 'skew' 
        //http://en.wikipedia.org/wiki/Harmonic_number
        for (int i = 1; i <= size; i++) {
            this.bottom += (1.0d / Math.pow(i, this.skew));
        }
    }

    /**
     * Method that returns a rank id between 0 and this.size (exclusive).
     * The frequency of returned rank id follows the Zipf distribution represented by this class.
     * @return a rank id between 0 and this.size.
     * @throws lptracegen.DistributionGenerator.DistributionException
     */

    public int next(){
        int rank = -1;
        double frequency = 0.0d;
        double dice = 0.0d;
        while (dice >= frequency) {
            rank = this.rnd.nextInt(this.size);
            frequency = getProbability(rank + 1);//(0 is not allowed for probability computation)
            dice = this.rnd.nextDouble();
        }
        return rank;
    }

    /**
     * Method that computes the probability (0.0 .. 1.0) that a given rank occurs.
     * The rank cannot be zero.
     * @param rank
     * @return probability that the given rank occurs (over 1)
     * @throws lptracegen.DistributionGenerator.DistributionException
     */
    public double getProbability(int rank) {
        if (rank == 0) {
            throw new RuntimeException("getProbability - rank must be > 0");
        }
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }

    /**
     * Method that returns a Zipf distribution
     * result[i] = probability that rank i occurs
     * @return the zipf distribution
     * @throws lptracegen.DistributionGenerator.DistributionException
     */
    public double[] getDistribution() {
        double[] result = new double[this.size];
        for ( int i = 1; i <= this.size; i++) { //i==0 is  not allowed to compute probability
            result[i - 1] = getProbability(i);
        }
        return result;
    }

    /**
     * Method that computes an array of length == this.size
     * with the occurrences for every rank i (following the Zipf)
     * result[i] = #occurrences of rank i
     * @param size
     * @return result[i] = #occurrences of rank i
     * @throws lptracegen.DistributionGenerator.DistributionException
     */
    public int[] getRankArray(int totalEvents) {
        int[] result = new int[this.size];
        for (int i = 0; i < totalEvents; i++) {
            int rank = next();
            result[rank] += 1;
        }
        return result;
    }
    
    public static void main(String[] args){
    	ZipfGenerator generator = new ZipfGenerator(100,3);
    	double[] values = generator.getDistribution();
    	System.out.println(values.length);
    	for(int i=0;i<values.length;i++)
    		System.out.println(values[i]);
    	
    }
    
    
}
