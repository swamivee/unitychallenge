package com.unity.challenge.common;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CommandLineParseUtil {

  private static final Log LOG = LogFactory.getLog(CommandLineParseUtil.class);

  public static String[] parse(String[] args) {
    CommandLineParser parser = new BasicParser();
    int noOfRecords = 100;
    Options options = new Options();
    options.addOption("i", "inputDirectory", true, "Input directory ");
    options.addOption("o", "outputDirectory", true, "output directory");
    options.addOption("f", "forceRun", true, "Force the run of MR");
    options.addOption("n", "noOfTopRecords", true, "Number of top records");

    String inputDirectory = null;
    String outputDirectory = null;
    boolean forceRun = false;
    try {
      CommandLine cmdLine = parser.parse(options, args);

      inputDirectory = (String) cmdLine.getOptionValue("i");
      outputDirectory = (String) cmdLine.getOptionValue("o");

      if (cmdLine.hasOption("n")) {
        noOfRecords = Integer.parseInt(cmdLine.getOptionValue("n"));
      }

      if (cmdLine.hasOption("f")) {
        forceRun = Boolean.parseBoolean(cmdLine.getOptionValue("f"));
      }
      LOG.info("===================== command arguments ===========================");
      LOG.info("input folder: " + inputDirectory);
      LOG.info("output folder: " + outputDirectory);
      LOG.info("noOfRecords: " + noOfRecords);
      LOG.info("===================== command arguments ===========================\n");
      String[] returnArgs = new String[4];
      returnArgs[0] = inputDirectory;
      returnArgs[1] = outputDirectory;
      returnArgs[2] = Integer.toString(noOfRecords);
      returnArgs[3] = Boolean.toString(forceRun);

      return returnArgs;
    } catch (Exception e) {
      LOG.error("Error passing input arguments:"
          + "-i<inputFolder>-o<outputFolder>-n<noofTopRecords>" + e.getMessage());
      System.exit(-1);
    }
    return null;

  }
}
