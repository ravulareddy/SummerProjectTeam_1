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
public class ExpediaRunner1 {
     
 //   private static  String inputPath = "hdfs://ec2-52-43-100-208.us-west-2.compute.amazonaws.com:8021/home/ubuntu/hadoop/train4.txt";
//  private static  String outputPath = "hdfs://ec2-52-43-100-208.us-west-2.compute.amazonaws.com:8021/home/ubuntu/hdfstmp/expedia/outputex/";
//  private static  String temp = "hdfs://ec2-52-43-100-208.us-west-2.compute.amazonaws.com:8021/home/ubuntu/hdfstmp/expedia/tempex/";

 //   private static final String OUTPUT_PATH = "intermediate_output";
    
  public static void main(String[] args) throws Exception {   
      
     String inputPath = "/Volumes/MacBackUp/trainfullData2.txt";
     String outputPath = "/Volumes/MacBackUp/expedia/outputExpedia/";
     String temp = "/Volumes/MacBackUp/expedia/temp2Exp/";
     
  //    String inputPath = "hdfs://ec2-52-43-100-208.us-west-2.compute.amazonaws.com/home/ubuntu/hadoop/train4.txt";
   //  String outputPath = "hdfs://ec2-52-43-100-208.us-west-2.compute.amazonaws.com/home/ubuntu/hdfstmp/expedia/outputex/";
  //  String temp = "hdfs://ec2-52-43-100-208.us-west-2.compute.amazonaws.com/home/ubuntu/hdfstmp/expedia/tempex/";
     
    JobClient client = new JobClient();
        JobConf conf = new JobConf(ExpediaRunner1.class);
        conf.setJobName("ExpediaMapper1");
        conf.setMapperClass(ExpediaMapper2.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setReducerClass(ExpediaReducer2.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(temp));

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
       // chaining  
       
       
       JobConf conf1 = new JobConf(ExpediaRunner1.class);
        conf1.setJobName("ExpediaMapper2");
        conf1.setMapperClass(ExpediaMapper3.class);
        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setReducerClass(ExpediaReducer3.class);
        FileInputFormat.setInputPaths(conf1, new Path(temp));
        FileOutputFormat.setOutputPath(conf1, new Path(outputPath));

     //   FileInputFormat.setInputPaths(conf1, new Path(OUTPUT_PATH));
     //   FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        
        client.setConf(conf1);
        try {
            JobClient.runJob(conf1);
        } catch (Exception e) {
            e.printStackTrace();
        } 
  }
}
