/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package U.CC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tejageetla
 */
public class ExpReducerDest  extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{ 
  
   public void reduce(Text key, Iterator<Text> values, 
                      OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
  
     reporter.setStatus(key.toString()); 
    
     String newKey ;
    // double count = 0;
     String hotel_clusterId = "";
     HashMap<String, Integer> hotelmap = new HashMap<String, Integer>();
      double total=0;
      StringBuilder sb = new StringBuilder();
     while(values.hasNext()) 
     { 
        StringBuilder toWrite = new StringBuilder(); 
        String page = ((Text)values.next()).toString(); 
        String[] valTokens =  page.split(" ");
        int bkcnt = Integer.valueOf(valTokens[2]);
        String distance = valTokens[0];
        String hotel_mrkt = valTokens[1];
        String isBooking = valTokens[2];
        int count = Integer.parseInt(valTokens[3]);
         String isPckg = valTokens[4];
        hotel_clusterId =  valTokens[5];
        
        newKey = key.toString() + " " + hotel_clusterId;
     //   toWrite.append(val);
        
     //    System.out.println("val"+newKey);
        
     if(isBooking.equalsIgnoreCase("1") && isPckg.equalsIgnoreCase("0"))
     {
     boolean check = checkIfAlreadyExist(hotelmap,hotel_clusterId);
     if(check)
     {
      int temp=hotelmap.get(hotel_clusterId);
        temp+= count;
        hotelmap.put(hotel_clusterId, temp);
     }
     else
     {
       hotelmap.put(hotel_clusterId, count);   
     }
     }
   }
       Set<Map.Entry<String, Integer>> hotelIdSet = hotelmap.entrySet();
     
       ArrayList<Map.Entry<String, Integer>> hotelClusterlist = new ArrayList<Map.Entry<String, Integer>>(hotelIdSet);
        Collections.sort( hotelClusterlist, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
        for(Map.Entry<String, Integer> entry:hotelClusterlist){
            sb.append(entry.getKey());
            sb.append(" ");
            sb.append(entry.getValue());
            sb.append(" ");
            }
     output.collect(key, new Text(sb.toString()));
     
   //    System.out.println("toWrite before val" + toWrite.toString());
     
 //    System.out.println("toWrite after val" + toWrite.toString());
  //   System.out.println("REducere >>>>> value "+ toWrite );
 //    output.collect(key, new Text(toWrite.toString())); 
     
   }
   
   public boolean checkIfAlreadyExist(HashMap<String,Integer> htlMap, String htlId)
   {
            if(htlMap.containsKey(htlId))
            {
            return true;
            }
            else
            {
            return false;
            }      
   }
    
}
