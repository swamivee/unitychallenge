package com.unity.challenge.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.PairWritable;


/**
 * Initial mapper to read the CSV validate the data and filter out the mapper records which
 * have +negative or 'NA' ArrDelay out
 * @author sveerama
 *
 */

public class AverageCSVMapper extends Mapper<LongWritable, Text, PairWritable, MetricPair> {
  private static final String HEADER = "YEAR";
  private static final String DOUBLE_REGEX = "(-?(\\d)+(\\.)?(\\d)*)";

  public static enum MapperCounters {
    EMPTY_LINE,
    INVALID_RECORD,
    FILTERED_RECORD,
    TOTAL_INPUT_RECORDS,
    TOTAL_OUTPUT_RECORDS,
    HEADER_RECORD,
    NO_DELAY_RECORDS
  };
  // value of the mapper
  private final static MetricPair metricPair = new MetricPair();
  //key to the mapper
  private final static PairWritable keyPair = new PairWritable();

  private static final int NO_OF_COLUMNS = 29;

  private static final String COLUMN_DELIMITER = ",";

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    context.getCounter(MapperCounters.TOTAL_INPUT_RECORDS).increment(1);
    if (value != null) {
      String line = value.toString().trim().toUpperCase();
      //Remove header
      if (line.toString().startsWith(HEADER)) {
        context.getCounter(MapperCounters.HEADER_RECORD).increment(1);
      } else {
        String[] columns = line.split(COLUMN_DELIMITER);
        if (columns.length != NO_OF_COLUMNS) {
          //if the line split is!=29 then assign it as invalid record
          context.getCounter(MapperCounters.INVALID_RECORD).increment(1);
        } else {
          String origin = columns[16].trim();
          String destination = columns[17].trim();
          //Check if the ArrDelay column is matching the regex of double
          if (columns[14].matches(DOUBLE_REGEX) && StringUtils.isNotBlank(origin)
              && StringUtils.isNotBlank(destination)) {

            Double timeDelay = Double.parseDouble(columns[14]);
            if (timeDelay <= 0) {
              //If the ArrDelay <=0 then there is no delay hence not interested in these records
              context.getCounter(MapperCounters.NO_DELAY_RECORDS).increment(1);
            } else {
              metricPair.set(timeDelay);
              keyPair.set(origin, destination);
              context.getCounter(MapperCounters.TOTAL_OUTPUT_RECORDS).increment(1);
              context.write(keyPair, metricPair);
            }
          } else {
            context.getCounter(MapperCounters.FILTERED_RECORD).increment(1);
          }
        }

      }
    } else {
      context.getCounter(MapperCounters.EMPTY_LINE).increment(1);
    }
  }

}
