/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package U.CC;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author tejageetla
 */
public class ExpDestFromUserCity {
    
    
    
     public static void main(String[] args) throws Exception { 
         String inputPath = "/Volumes/MacBackUp/train4.txt";
     String outputPath = "/Volumes/MacBackUp/expedia/outDestRec/";
  //   String temp = "/Volumes/MacBackUp/expedia/temp2Exp/";
     
         JobClient client = new JobClient();
        JobConf conf = new JobConf(ExpDestFromUserCity.class);
        conf.setJobName("ExpediaDestHotelRec");
        conf.setMapperClass(ExpMapperDest.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setReducerClass(ExpReducerDest.class);
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
