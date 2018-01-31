package PIT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class Extract {

	public static void main(String[] args) throws IOException {
		

String path = "/home/atoshum/Downloads/014";
File folder = new File(path);
File[] listOfFiles = folder.listFiles();

BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum/micTaxi.csv"));

for (File file : listOfFiles) {
    if (file.isFile()) {
        
      Scanner sc = new Scanner(new File(path+"/"+file.getName()));
      while (sc.hasNextLine())
      {
    	   
    	  
    	  String[] line = sc.nextLine().split(","); 
    	  bw.write(line[2] + " "+ line[3]+"\n");
      }
      sc.close();
    }
}
bw.flush();
bw.close();


	}

}
