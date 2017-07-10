/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package U.CC;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tejageetla
 */
public class ExpediaMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 
  
  
   public void map(LongWritable key, Text value, 
                   OutputCollector output, Reporter reporter) throws IOException
{
     reporter.setStatus(key.toString());    
     String page = value.toString();
     String[] arr = page.split("\\s");
     
     String userDestHot =  arr[0] + "," + arr[1] + ":" + arr[2];
  //   System.out.println("userDestHot>>> "+userDestHot);
     String toEmit = arr[3] + " " + arr[4];
     
   //   System.out.println("toEmit>>> "+toEmit);
     
     output.collect(new Text(userDestHot), new Text(toEmit));
     
}
    
}
