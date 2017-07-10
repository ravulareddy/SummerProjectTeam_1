/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package U.CC;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tejageetla
 */
public class ExpediaViewer  { 
  
 public static class ExpedMapView extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {   
   public void map(LongWritable key, Text value, 
                   OutputCollector output, Reporter reporter) throws IOException
    {
         // Prepare the input data. 
         String page = value.toString(); 
         String[] arr = page.split("\\s");
         String emitKey = arr[0];
         int startIndex = page.indexOf('[');
         int endIndex = page.indexOf(']');
        String emitVal = page.substring(startIndex + 1, endIndex);
         emitVal = emitVal.replaceAll("0000", " ");
         String newVal = "[" + emitVal + "]";
         
        output.collect(new Text(emitKey),new Text(newVal));

    }
 } 
   public static void main(String[] args) throws Exception { 
       
       
      String inputPath = "/Volumes/MacBackUp/mahFullOutput.txt";
     String outputPath = "/Volumes/MacBackUp/expedia/mahFullOutput_format";
     
  //          String inputPath = "/user/tejageetla/inputExpedia/trainfullData2.txt";
  //   String outputPath = "/user/tejageetla/expedia/Dest_lngth_UsrCityReco/";
  //   String temp = "/Volumes/MacBackUp/expedia/temp2Exp/";
     
        JobClient client = new JobClient();
        JobConf conf = new JobConf(ExpediaViewer.class);
        conf.setJobName("ExpediaDestHotelRecViewer");
        conf.setMapperClass(ExpedMapView.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        } 
       
     } 
    
}
