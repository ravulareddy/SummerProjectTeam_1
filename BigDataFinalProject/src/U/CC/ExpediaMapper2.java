/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package U.CC;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
public class ExpediaMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 
  
  
   public void map(LongWritable key, Text value, 
                   OutputCollector output, Reporter reporter) throws IOException
{
     // Prepare the input data. 
     String page = value.toString(); 
     
          String[] arr = page.split(",");
       String datime = arr[1];   
       int book_year = Integer.parseInt(datime.substring(0, 4));
       int book_month = Integer.parseInt(datime.substring(5,7));
       String user_location_city = arr[6];
       String orig_destination_distance = arr[7];
       String user_id = arr[8];
       String is_package = arr[10];
       String srch_destination_id = arr[17];
       int is_booking = Integer.parseInt(arr[19]);
       int count = Integer.parseInt(arr[20]);
       String hotel_country = arr[22];
       String hotel_market = arr[23];
       String hotel_cluster = arr[24];
       String timeStamp = "";
      try
     {
       SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parseDate = format.parse(arr[1].substring(0,19));
    //    System.out.println(parseDate);
       Calendar c = Calendar.getInstance();
            c.setTime(parseDate);
            long time = c.getTimeInMillis();
            timeStamp = String.valueOf(time);
   //      System.out.println("ts"+time);
     }
     catch(ParseException pe)
     {
      pe.printStackTrace();
     } 
  /*     
     
      double yrmnth = ((book_year - 2012) * 12 + (book_month - 12)) * ((book_year - 2012) * 12 + (book_month - 12));
      
      System.out.println(">>>>> " + yrmnth);
      double yrmnth1 = yrmnth * (3 + 16 * is_booking);
       System.out.println(">>>>> " + yrmnth1);
      double yrmnth2 = 3 + 5.1 * is_booking;
       System.out.println(">>>>> " + yrmnth2); */
     
     StringBuilder builder = new StringBuilder();
     builder.append(hotel_cluster);
     builder.append(" ");
     builder.append(is_booking);
     builder.append(" ");
     builder.append(count);
     builder.append(" ");
     builder.append(user_id);
     builder.append(" ");
     builder.append(timeStamp);
    
             
     output.collect(new Text(srch_destination_id), new Text(builder.toString())); 

}
   
   
    
}
