/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package U.CC;

 import java.io.IOException; 
 import java.util.Iterator; 
  
 import org.apache.hadoop.io.Text; 
 import org.apache.hadoop.mapred.MapReduceBase; 
 import org.apache.hadoop.mapred.OutputCollector; 
 import org.apache.hadoop.io.DoubleWritable;
 import org.apache.hadoop.mapred.Reducer; 
 import org.apache.hadoop.mapred.Reporter; 
 import java.lang.StringBuilder; 

/**
 *
 * @author tejageetla
 */
public class ExpediaReducer2  extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{ 
  
   public void reduce(Text key, Iterator<Text> values, 
                      OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
  
     reporter.setStatus(key.toString()); 
    
     String newKey ;
    // double count = 0;
     String hotel_clusterId = "";
      double total=0;
     while(values.hasNext()) 
     { 
        StringBuilder toWrite = new StringBuilder(); 
        String page = ((Text)values.next()).toString(); 
        String[] valTokens =  page.split(" ");
        int bkcnt = Integer.valueOf(valTokens[2]);
        String isBooking = valTokens[1];
        String userId = valTokens[3];
        String timeStamp = valTokens[4];
        hotel_clusterId =  valTokens[0];
        
        newKey = key.toString() + " " + hotel_clusterId;
     //   toWrite.append(val);
        
     //    System.out.println("val"+newKey);
        
        if(isBooking.equalsIgnoreCase("1"))
        {
         total =   bkcnt*5;     
        }
        else if(isBooking.equalsIgnoreCase("0"))
        {
         total =  bkcnt*1;     
        }
        
     DoubleWritable i = new DoubleWritable(total);
     String num = (i).toString(); 
     toWrite.append(newKey);
     toWrite.append(" ");
  //   toWrite.append("#");
     toWrite.append(num);
     toWrite.append(" ");
     toWrite.append(timeStamp);
     output.collect(new Text(userId), new Text(toWrite.toString())); 
     }
     
     
   //    System.out.println("toWrite before val" + toWrite.toString());
     
 //    System.out.println("toWrite after val" + toWrite.toString());
  //   System.out.println("REducere >>>>> value "+ toWrite );
 //    output.collect(key, new Text(toWrite.toString())); 
     
   }
    
}
