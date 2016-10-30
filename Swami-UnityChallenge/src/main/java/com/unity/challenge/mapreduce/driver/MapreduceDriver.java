package com.unity.challenge.mapreduce.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import com.unity.challenge.common.CommandLineParseUtil;
import com.unity.challenge.mapreduce.driver.AirlineDelayAverageMR;


/**
 * Main Mapreduce driver, first calls the @AirlineDelayAverageMR and then @TopNAirlineDelayMR
 * @author sveerama
 *
 */
public class MapreduceDriver {
  private static final Log LOG = LogFactory.getLog(MapreduceDriver.class);

  public static void main(String[] args) throws Exception {
    try {

      String[] parsedArgs = CommandLineParseUtil.parse(args);
      int exitCode = ToolRunner.run(new AirlineDelayAverageMR(), parsedArgs);
      LOG.info(exitCode);

      if (exitCode == 0) {
        exitCode = ToolRunner.run(new TopNAirlineDelayMR(), parsedArgs);
      }
      System.exit(exitCode);
    } catch (Throwable t) {
      LOG.error(t.getMessage());
      LOG.error("Something went wrong when running mapreduce ", t);
      System.exit(-1);
    }
  }
}
