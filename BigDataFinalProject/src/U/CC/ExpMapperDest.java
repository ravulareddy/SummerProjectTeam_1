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
public class ExpMapperDest extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 
  
  
   public void map(LongWritable key, Text value, 
                   OutputCollector output, Reporter reporter) throws IOException
{
     // Prepare the input data. 
     String page = value.toString(); 
     String[] arr = page.split(",");
        
       String hotel_market = arr[23];
       String hotel_cluster = arr[24];
       String user_country = arr[4];
       String user_region = arr[5];
       String user_city = arr[6];
       String is_package = arr[10];
       String orig_destination_distance = arr[7];
       int is_booking = Integer.parseInt(arr[19]);
       int count = Integer.parseInt(arr[20]);
       String hotel_country = arr[22];
       
     StringBuilder builder = new StringBuilder();
     builder.append(orig_destination_distance);
     builder.append(" ");
     builder.append(hotel_market);
     builder.append(" ");
     builder.append(is_booking);
     builder.append(" ");
     builder.append(count);
     builder.append(" ");
     builder.append(is_package);
     builder.append(" ");
     builder.append(hotel_cluster); 
     
     String emitkey = user_country+ "_" + user_region + "_" + user_city +"_" + hotel_country ; 
             
     output.collect(new Text(emitkey), new Text(builder.toString())); 
}
   
}
