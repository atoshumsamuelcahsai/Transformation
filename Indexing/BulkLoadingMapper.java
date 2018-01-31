package PIT.Indexing;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import AssignDataSetToGridCell.RowKeyCreator;
import PythiaHbase.AscendingHeapSort;
import PythiaHbase.Helper;
import QuadTree.Box;
import QuadTree.CellSummary;
import QuadTree.Point;
import QuadTree.QuadTree;
import XmlParser.Parameters;
import XmlParser.Parser;
 



public   class BulkLoadingMapper extends Mapper<LongWritable, Text,ImmutableBytesWritable, KeyValue> {
     
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
	  String address = value.toString().split("\t")[0];
			
	  ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(address));
	        
	  KeyValue myKeyValue = new KeyValue(rowKey.get(), Bytes.toBytes("C"), Bytes.toBytes("x"), Bytes.toBytes(value.toString().substring(value.toString().indexOf("\t")+1)));
		// KeyValue myKeyValue = new KeyValue(rowKey.get(), Bytes.toBytes("C"), Bytes.toBytes("x"), Bytes.toBytes(value.toString()) );
	  context.write(rowKey, myKeyValue);  
}
  
 







}