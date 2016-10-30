package com.unity.challenge.mapreduce.driver;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.unity.challenge.common.DescendingSortComparator;
import com.unity.challenge.mapper.TopNRecordsMapper;
import com.unity.challenge.reduce.TopNRecordsReducer;


/**
 * MapReduce Driver for getting the top x records.. If there is only reducer file from the @AirlineDelayAverageMR
 * then it just renames the folder
 * 
 * Mapper= @TopNRecordsMapper
 * MapperInputKey=@LongWritable
 * MapperInputValue=@Text
 * MapperOutputKey=@DoubleWritable
 * MapperOutputValue=@Text
 * 
 * Reducer= @TopNRecordsReducer
 * ReducerOutputKey=@Text
 * ReducerOutputValue=@NullWritable
 * 
 * @author sveerama
 *
 */

public class TopNAirlineDelayMR extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TopNAirlineDelayMR.class);
  private static final String TOP_RECORDS_COUNT = "TOP_RECORDS";

  @Override
  public int run(String[] args) throws Exception {
    String outputDirectory = args[1];
    int noOfRecords = Integer.parseInt(args[2]);
    boolean forceRun = Boolean.parseBoolean(args[3]);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(outputDirectory + "_average");
    if (!fs.exists(inputPath)) {
      LOG.error("The input path doesnt exists:" + inputPath);
      System.exit(1);
    }
    LOG.info("Input Path:" + inputPath.toString());

    Path outputPath = new Path(outputDirectory);
    LOG.info("Output Path:" + outputPath.toString());
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    if (!forceRun && !isFileCountMoreThanOne(fs, inputPath)) {
      LOG.info("Moving files from: " + inputPath + " to " + outputPath);
      return fs.rename(inputPath, outputPath) ? 0 : 1;
    }

    conf.setInt(TOP_RECORDS_COUNT, noOfRecords);
    Job topNJob = Job.getInstance(conf, "TopNAirlineDelayMR");
    topNJob.setJarByClass(TopNAirlineDelayMR.class);
    topNJob.setMapperClass(Mapper.class);
    topNJob.setMapperClass(TopNRecordsMapper.class);
    topNJob.setSortComparatorClass(DescendingSortComparator.class);
    topNJob.setMapOutputKeyClass(DoubleWritable.class);
    topNJob.setMapOutputValueClass(Text.class);
    topNJob.setOutputKeyClass(Text.class);
    topNJob.setOutputValueClass(NullWritable.class);
    topNJob.setInputFormatClass(TextInputFormat.class);
    topNJob.setNumReduceTasks(1);
    topNJob.setReducerClass(TopNRecordsReducer.class);

    FileInputFormat.addInputPath(topNJob, inputPath);
    FileOutputFormat.setOutputPath(topNJob, outputPath);

    int success = topNJob.waitForCompletion(true) ? 0 : 1;
    if (success == 0) {
      LOG.info("Intermediate Path Delete:" + inputPath.toString());
      if (fs.exists(inputPath)) {
        fs.delete(inputPath, true);
      }
    }
    return success;
  }

  private boolean isFileCountMoreThanOne(FileSystem fs, Path inputPath)
      throws FileNotFoundException, IOException {
    int fileCount = 0;
    for (FileStatus status : fs.listStatus(inputPath)) {
      if (status.getPath().getName().startsWith("part")) {
        fileCount++;
      }
      if (fileCount > 1) {
        return true;
      }
    }
    return false;
  }
}
