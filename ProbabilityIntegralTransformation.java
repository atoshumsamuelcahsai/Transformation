package PIT;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class ProbabilityIntegralTransformation  implements java.io.Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	RealVector mean;

	RealMatrix covariance;
    
	RealMatrix sqrtOfInverseCovarianceMatrix;

	double[] point;
	
//	RealMatrix aInverse;

	ArrayList<double[]> data;

	int n = -1;;
	private  Random fRandom;

	public ProbabilityIntegralTransformation(double[][] covMatrix, double[] meanVector)
	{
        this.covariance = MatrixUtils.createRealMatrix(covMatrix);
		
		//this.aInverse  = new LUDecomposition(covariance).getSolver().getInverse();
		
		this.sqrtOfInverseCovarianceMatrix = new EigenDecomposition(new LUDecomposition(covariance).getSolver().getInverse()).getSquareRoot();
		
		this.mean = new ArrayRealVector(meanVector);
	}


//	public void generateData()
//	{
//		if (this.n > 0 || this.mean == null || this.covariance == null )  throw new NullPointerException(" mean or covariance or number of data items are not initialized");
//
//		for(int i= 0; i < n; i++) 
//		{
//			double[] vec = new double[2];
//
//			int count = 0;
//
//			while (count < 2)
//			{
//				double gen = fRandom.nextGaussian();
//
//				vec[count] = gen;
//
//				count++;
//
//			}
//              
//			//double col_1 = this.mean[0] + ( ( vec[0] * covariance[0][0] ) + ( vec[1] * covariance[1][0] ) );
//			//double col_2 = this.mean[1] + ( ( vec[0] * covariance[0][1] ) + ( vec[1] * covariance[1][1] ) );
//
//			//this.data.add(new double[] {col_1,col_2});
//
//		}
//	}

	public double[] standaraizeAndremoveCorrelation(double[] point)
	{
		//double[][] sqrOFInverseCovarianceMatrix = getSQRMatrix();
		
		for (int i = 0; i < point.length; i++)
		{
			point[i]  =  point[i] - this.mean.getEntry(i);
		}
				
		//return multiplyMatrixVector(sqrOFInverseCovarianceMatrix, point);
		return this.sqrtOfInverseCovarianceMatrix.operate(point);
	}

	
public double[] transformToOrginal(double[][] point )
{
		double[] orginal = new double[this.mean.getDimension()];
	
		double[][] mul = MatrixUtils.createRealMatrix(point).multiply(new LUDecomposition(sqrtOfInverseCovarianceMatrix).getSolver().getInverse()).getData();
				
		//multiplyMatrix(point, this.aInverse);
		
 
		
	 
		for (int x = 0 ; x < this.mean.getDimension(); x++)
		{
			 
			orginal[x] = mul[0][x] + this.mean.getEntry(x);
		//	System.out.println(" ======= "+ orginal[x]);
						
		}
		
		return orginal;
}

public double[] getVector()
{
		return mean.toArray();
}

public void setMean(double[] vector)
{
		this.mean = new ArrayRealVector(vector);
}

public double[][] getMatrix()
{
		return covariance.getData();
}

	
public double pnorm1(double x)
{
	    int neg = (x < 0d) ? 1 : 0;
	    
	    if ( neg == 1)   x *= -1d;

	    double t = (1d / ( 1d + 0.2316419 * x));
	    
	    
	   // double y = (((( 1.330274429 * t - 1.821255978) * t + 1.781477937) *  t - 0.356563782) * t + 0.319381530) * t;
	    double y = (0.319381530 * t) + (-0.356563782 * Math.pow(t, 2)) + (1.781477937  * Math.pow(t, 3)) + (-1.821255978 * Math.pow(t, 4)) + (1.330274429 *  Math.pow(t, 5));
	    
	    
	    y = 1.0 - ( (0.398942280401 * Math.exp(-0.5 * x * x) * y) + 0.000000075);
 
	    return (1d - neg) * y + neg * (1d - y);
	}

public double pnorm(double z)
{
	  int neg = (z < 0d) ? 1 : 0;
	  
	  if ( neg == 1)   z *= -1d;
	  
	  double ltone=7.0, utzero=18.66, con=1.28, a1 = 0.398942280444,  a2 = 0.399903438504,  a3 = 5.75885480458,    a4 =29.8213557808,
	         a5 = 2.62433121679 ,     a6 =48.6959930692,              a7 = 5.92885724438,   b1 =0.398942280385,    b2 =3.8052e-8,
	         b3 =1.00000615302,       b4 =3.98064794e-4,              b5 =1.986153813664,   b6 =0.151679116635,    b7 =5.29330324926,
	         b8 =4.8385912808,        b9 =15.1508972451,              b10=0.742380924027,   b11=30.789933034,      b12=3.99019417011;
	  
	   double y, alnorm = 0;
	    if(z<=ltone || neg ==1 && z<=utzero) 
	    {
	      y=0.5*z*z;
	      
	      if(z>con) 
	      {
	        alnorm=b1*Math.exp(-y)/(z-b2+b3/(z+b4+b5/(z-b6+b7/(z+b8-b9/(z+b10+b11/(z+b12))))));
	      }
	      else 
	      {
	             alnorm=0.5-z*(a1-a2*y/(y+a3-a4/(y+a5+a6/(y+a7))));
	      }
	    }
	    else {
	      alnorm=0;
	    }
	    if(!(neg==1)) alnorm= 1-alnorm ;
	    return(alnorm);
}
	
public double qnorm1(double p)
{
		 if(p<0 || p>1) throw new IllegalArgumentException("Illegal argument "+p+" for qnorm(p).");
		 
		 
		 double split=0.425;
		 double a0=  2.50662823884, a1=-18.61500062529,  a2= 41.39119773534, a3=-25.44106049637,
		        b1= -8.47351093090, b2= 23.08336743743,  b3=-21.06224101826, b4=  3.13082909833,
		        c0= -2.78718931138, c1= -2.29796479134,  c2=  4.85014127135, c3=  2.32121276858,
		        d1=  3.54388924762, d2=  1.63706781897;
		 double q=p-0.5;
		    
		 double r,ppnd;
		 
		 if(Math.abs(q)<=split) 
		 {
		      r=q*q;
		      ppnd=q*(((a3*r+a2)*r+a1)*r+a0)/((((b4*r+b3)*r+b2)*r+b1)*r+1);		     
		  }
		  else
		  {
			  
		      r=p;
		      if(q>0) r=1-p;
		     
		      if(r>0)
		      {
		    	 r=Math.sqrt(-Math.log(r));
		        	      
		        ppnd=(((c3*r+c2)*r+c1)*r+c0)/((d2*r+d1)*r+1);
		        	         
		        if(q<0) ppnd=-ppnd;
		      }
		      else 
		      {
		        ppnd=0;
		      }
		    }
		 
		    return(ppnd);
}
	

	
public double qnorm(double p)
{
		 if(p<0 || p>1)      throw new IllegalArgumentException("Illegal argument "+p+" for qnorm(p).");
		 double split  =  0.425;
		 double val = 0;
		 double r = 0.0;
		 double q  =  p-0.5;
		 
		 if(Math.abs(q)  <= split) 
		 {
		      r= 0.180625 - (q*q);
		      
		      val =  q * (((((((r * 2509.0809287301226727 +
		                         33430.575583588128105) * r + 67265.770927008700853) * r +
		                       45921.953931549871457) * r + 13731.693765509461125) * r +
		                     1971.5909503065514427) * r + 133.14166789178437745) * r +
		                   3.387132872796366608)
		              / (((((((r * 5226.495278852854561 +
		                       28729.085735721942674) * r + 39307.89580009271061) * r +
		                     21213.794301586595867) * r + 5394.1960214247511077) * r +
		                   687.1870074920579083) * r + 42.313330701600911252) * r + 1.00);
		    }
		    else 
		    {
		    	
		     	 if(q > 0) 
		    	 {
		    	    r= 1-p ;
		    	 }
		    	 else
		    	 {
		    		r = p;
		    	 }
		    
		    	
		    	 
		    	r = Math.sqrt(-1* Math.log(r) ) ;
		    	 
		     //	System.out.println(r + " ====================== ");
		     
//		     if (r <= 5.00) 
//		     { /* <==> min(p,1-p) >= exp(-25) ~= 1.3888e-11 */
//		    	 //System.out.println(r + " ====================== ");
//		    	 r += -1.6;
//		         val = (((((((r * 7.7454501427834140764e-4 +
//		                       0.0227238449892691845833) * r + 0.24178072517745061177) *
//		                     r + 1.27045825245236838258) * r +
//		                    3.64784832476320460504) * r + 5.7694972214606914055) *
//		                  r + 4.6303378461565452959) * r +
//		                 1.42343711074968357734)
//		                / (((((((r *
//		                         1.05075007164441684324e-9 + 5.475938084995344946e-4) *
//		                        r + 0.0151986665636164571966) * r +
//		                       0.14810397642748007459) * r + 0.68976733498510000455) *
//		                     r + 1.6763848301838038494) * r + 2.05319162663775882187) * r + 1.00);
//		         //System.out.println(r + " +++++++++++++++++++++++++++ ");
//		       }
//		       else
//		       { /* very close to  0 or 1 */
		                 r += -5.00;
		                 val = (((((((r * 2.01033439929228813265e-7 +
		                            2.71155556874348757815e-5) * r +
		                           .0012426609473880784386) * r + .026532189526576123093) *
		                         r + .29656057182850489123) * r +
		                        1.7848265399172913358) * r + 5.4637849111641143699) *
		                      r + 6.6579046435011037772)
		                     / (((((((r *
		                              2.04426310338993978564e-15 + 1.4215117583164458887e-7)*
		                             r + 1.8463183175100546818e-5) * r +
		                            7.868691311456132591e-4) * r + .0148753612908506148525)
		                          * r + .13692988092273580531) * r + .59983220655588793769) * r + 1.00);
//		        }
		     
		      if(q < 0.0) val = -val;
		    }
		 
		    return(val);
}
	
	
//public static void main(String[] args) 
//{
//		ProIntegeralTransformation mr = new ProIntegeralTransformation();
//		
//		double[][] test = new double[2][2];
//		
//		test[0][0] =  2.0;
//		
//	    test[0][1] = 0.9;
//		
//	    test[1][0] = 0.9;
//		
//	    test[1][1] = 2;
//				
//		
//	 	
//	 	//mr.getSQRMatrix();
//	 	
//	 	mr.setCovarianceMatrix(test);
//	 	mr.setMean(new double[]{7.00, 7.00});
//	 	
//	 	double [][] tes  = mr.inverseMatrix(test);
//	 	
//	  
//	 	
//	 	double[] t = mr.standaraizeAndremoveCorrelation(new double[] {5,5});
//	 	double[] m = mr.getVector();
//	 	System.out.println("point 5, 5");
//	 	System.out.println("stundarized and independent "+t[0]+ "\t"+ t[1]);
//	  
//	 	System.out.println(mr.pnorm(t[0]));
//	 	System.out.println(mr.pnorm(t[1]) );
//	 	
//	 	//System.out.println("this is the inverse of cdf_x: "+ mr.inverseCDF(x));
//	 	//System.out.println("this is the inverse of cdf_y: "+ mr.inverseCDF(y));
//
//	}


}
