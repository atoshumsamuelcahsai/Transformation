package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class MainReader {

	public static void main(String[] args) throws IOException
	{
		 HashMap<Integer, ArrayList<String>> test = new HashMap<Integer, ArrayList<String>>(); 
		 Scanner sc = new Scanner(new File("/home/atoshum/out.csv"));
		 while(sc.hasNext())
		 {
			 String[] line = sc.nextLine().split(",");
			 int id = Integer.valueOf(line[2]);
			 if (test.containsKey(id))
			 {
				 test.get(id).add(line[0]+","+line[1]);
			 }
			 else
			 {  
				 ArrayList<String> ls = new ArrayList<String>();
				 ls.add(line[0]+","+line[1]);
				 test.put(id,ls);
			 }
		 }
		 for(int key : test.keySet())
		 {
		  BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/Cluster/"+key+".csv"));
		  for(String p : test.get(key))
		  {
			  bw.write(p+"\n");
		  }
		  bw.flush();
		  bw.close();
		 }
		 
		 
	}

}
