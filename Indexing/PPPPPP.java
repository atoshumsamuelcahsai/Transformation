package PIT.Indexing;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PPPPPP {

	public static void main(String[] args) throws IOException
	{
		 DataInputStream input = new DataInputStream(new FileInputStream("/home/atoshum/Downloads/BJTaxi.05.09.with.occupancy.bit/output_0501.dat"));
		 
		 char c;
		 
		 while ( ( c = input.readChar() ) != -1)
   		{
			 System.out.print(c);
   		}
		 input.close();
		 

	}

}
