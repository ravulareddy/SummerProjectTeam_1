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
 import org.apache.hadoop.mapred.Reducer; 
 import org.apache.hadoop.mapred.Reporter; 
 import java.lang.StringBuilder; 

/**
 *
 * @author tejageetla
 */
public class ExpediaReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{ 
  
   public void reduce(Text key, Iterator<Text> values, 
                      OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
  
     reporter.setStatus(key.toString()); 
    long timestamp = 0;
    long timestmp =0;
    // double count = 0;
     String hotel_clusterId = "";
      double total=0;
     while(values.hasNext()) 
     { 
        StringBuilder toWrite = new StringBuilder(); 
        String page = ((Text)values.next()).toString(); 
        String[] valTokens =  page.split(" ");
        double score = Double.valueOf(valTokens[0]);
    //     System.out.println("score  : "+score);
        total+= score;
        timestmp = Long.parseLong(valTokens[1]);
        if(timestmp>timestamp)
        {
         timestamp = timestmp;   
        }
     }
    
     String toWrite = String.valueOf((int)total) ;
             //+ " " + String.valueOf(timestamp);
     output.collect(key, new Text(toWrite));
   }
    
}
