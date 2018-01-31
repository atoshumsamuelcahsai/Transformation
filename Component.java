package PIT;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class Component 
{
	RealVector mean;

	RealMatrix covariance;
	
	RealMatrix sqrtOfInverseCovarianceMatrix;
	
	double lamda;
	
	public Component(double[][] covMatrix, double[] meanVector, double l)
	{
		this.mean = new ArrayRealVector(meanVector);
		
		this.covariance = MatrixUtils.createRealMatrix(covMatrix);
		
		this.sqrtOfInverseCovarianceMatrix = new EigenDecomposition(new LUDecomposition(covariance).getSolver().getInverse()).getSquareRoot();
		
		this.lamda = l;
	}
	
	public double[] standaraizeAndremoveCorrelation(double[] point)
	{
						
		for (int i = 0; i < this.mean.getDimension(); i++)
		{
			point[i]  =  (point[i] - this.mean.getEntry(i)) ;
		}
		 
		return this.sqrtOfInverseCovarianceMatrix.operate(point);
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
		    
		    return(   alnorm);
	}
	
	
	public double pnorm1(double x)
	{
		    int neg = (x < 0d) ? 1 : 0;
		    
		    if ( neg == 1)   x *= -1d;

		    double t = (1d / ( 1d + 0.2316419 * x));
		    
		    
		   // double y = (((( 1.330274429 * t - 1.821255978) * t + 1.781477937) *  t - 0.356563782) * t + 0.319381530) * t;
		    double y = (0.319381530 * t) + (-0.356563782 * Math.pow(t, 2)) + (1.781477937  * Math.pow(t, 3)) + (-1.821255978 * Math.pow(t, 4)) + (1.330274429 *  Math.pow(t, 5));
		    
		    
		    y = 1.0 - ( (0.398942280401 * Math.exp(-0.5 * x * x) * y) + 0.000000075);
	        
		    return ((1d - neg) * y + neg * (1d - y)) * this.lamda;
	}

	public double[] getMean() {
		return mean.toArray();
	}

	public void setMean(RealVector mean) {
		this.mean = mean;
	}

	public double[][] getCovariance() {
		return covariance.getData();
	}

	public void setCovariance(RealMatrix covariance) {
		this.covariance = covariance;
	}

	public double getLamda() {
		return lamda;
	}

	public void setLamda(double lamda) {
		this.lamda = lamda;
	}
	
	
	
	 

}
