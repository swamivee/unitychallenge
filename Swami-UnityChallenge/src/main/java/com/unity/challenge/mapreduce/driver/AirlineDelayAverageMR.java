package com.unity.challenge.mapreduce.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.PairWritable;
import com.unity.challenge.mapper.AverageCSVMapper;
import com.unity.challenge.reduce.AverageCSVCombiner;
import com.unity.challenge.reduce.AverageCSVReduce;


/**
 * Average delay MR driver it reads the input file and outputs the average for each reducer
 * based on the TOP_RECORDS. For ex: if the TOP_RECORDS=100 then top 100 records by arrival delay are outputted
 * to each reducer file
 * 
 * Data from this output is compressed to GZ 
 * 
 * Mapper= @AverageCSVMapper
 * MapperInputKey=@LongWritable
 * MapperInputValue=@Text
 * MapperOutputKey=@PairWritable
 * MapperOutputValue=@MetricPair
 * 
 * Reducer= @AverageCSVReduce
 * ReducerOutputKey=@Text
 * ReducerOutputValue=@NullWritable
 * 
 * Combiner=@AverageCSVCombiner
 * 
 * 
 * @author sveerama
 *
 */
public class AirlineDelayAverageMR extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(AirlineDelayAverageMR.class);
  private static final String TOP_RECORDS_COUNT = "TOP_RECORDS";

  @Override
  public int run(String[] args) throws Exception {
    String inputDirectory = args[0];
    String outputDirectory = args[1];
    int noOfRecords = Integer.parseInt(args[2]);

    Configuration conf = getConf();
    conf.setInt(TOP_RECORDS_COUNT, noOfRecords);
    Job averageJob = Job.getInstance(conf, "AirlineDelayAverageMR");
    averageJob.setJarByClass(AirlineDelayAverageMR.class);
    averageJob.setMapperClass(AverageCSVMapper.class);
    averageJob.setReducerClass(AverageCSVReduce.class);
    averageJob.setMapOutputKeyClass(PairWritable.class);
    averageJob.setMapOutputValueClass(MetricPair.class);
    averageJob.setOutputKeyClass(Text.class);
    averageJob.setOutputValueClass(NullWritable.class);
    averageJob.setInputFormatClass(TextInputFormat.class);
    averageJob.setCombinerClass(AverageCSVCombiner.class);
    FileOutputFormat.setCompressOutput(averageJob, true);
    FileOutputFormat.setOutputCompressorClass(averageJob, GzipCodec.class);
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(inputDirectory);
    if (!fs.exists(inputPath)) {
      LOG.error("The input path doesnt exists:" + inputDirectory);
      System.exit(1);
    }
    LOG.info("Input Path:" + inputPath.toString());
    FileInputFormat.addInputPath(averageJob, inputPath);

    Path outputPath = new Path(outputDirectory + "_average");
    LOG.info("Output Path:" + outputPath.toString());
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(averageJob, outputPath);

    return averageJob.waitForCompletion(true) ? 0 : 1;

  }
}
