/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;

import org.apache.hadoop.util.ReflectionUtils;

import org.junit.Test;
import java.util.*;
import static junit.framework.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TestFixedLengthInputFormat {

  private static final Log LOG = 
    LogFactory.getLog(TestFixedLengthInputFormat.class.getName());

  private static Configuration defaultConf = new Configuration();

  private static FileSystem localFs = null; 

  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      // our set of chars
      chars = ("abcdefghijklmnopqrstuvABCDEFGHIJKLMN OPQRSTUVWXYZ1234567890)(*&^%$#@!-=><?:\"{}][';/.,']").toCharArray();
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir = 
    new Path(new Path(System.getProperty("test.build.data", "."), "data"),
        "TestKeyValueFixedLengthInputFormat");


  // some chars for the record data
  private static char[] chars;
  private static Random charRand = new Random();


  // each test file contains records of total bytes length
  // each record starts with the ! char and ends with the ! char for testing
  // and record start/end verification, this returns a Map of the record position
  // pointing to the value @ that position. This Map is to be used in conjuction
  // with the test that uses the DEFAULT KEY configuration of FixedLengthInputFormat
  // (which is the record start position in the InputSplit)
  private Map<Long,String> writeDefaultKeyDummyFile(Path targetFile, int totalBytes, 
      int recordLen) throws Exception {

    Map<Long,String> valMap = new HashMap<Long,String>();

    // create a file X records
    Writer writer = new OutputStreamWriter(localFs.create(targetFile));
    try {
      long recordBytesWritten = 0;
      long lastRecordStart = 0;
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < totalBytes; i++) {


        recordBytesWritten++;

        if (i==lastRecordStart) {
          sb.append('!'); // start of a record 
        } else if (recordBytesWritten==recordLen) {
          sb.append('!'); // end of a record
        } else {
          // filler
          sb.append(chars[charRand.nextInt(chars.length)]);
        }

        if (recordBytesWritten == recordLen) {
          valMap.put(lastRecordStart,sb.toString());
        //  LOG.info("KEY=" + lastRecordStart + 
       //           " VALUE=" +sb.toString());
          writer.write(sb.toString());
          sb = new StringBuffer();
          lastRecordStart = i+1;
          recordBytesWritten=0;
        }


      }
    } finally {
      writer.close();
    }

    return valMap;
  }
  
  // each test file contains records of total bytes length
  // each record starts with the ! char and ends with the ! char for testing
  // and record start/end verification, this returns a Map of the custom KEY
  // which maps to the value @ that key. This Map is to be used in conjuction
  // with the test that uses the CUSTOM KEY configuration of FixedLengthInputFormat
  // (which defines the key by the byte start/end positions)
  private Map<String,String> writeCustomKeyDummyFile(Path targetFile, int totalBytes, 
      int recordLen, int keyStartAt, int keyEndAt) throws Exception {

    Map<String,String> valMap = new HashMap<String,String>();

    // create a file X records
    Writer writer = new OutputStreamWriter(localFs.create(targetFile));
    try {
      long recordBytesWritten = 0;
      long lastRecordStart = 0;
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < totalBytes; i++) {


        recordBytesWritten++;

        // filler
        sb.append(chars[charRand.nextInt(chars.length)]);


        if (recordBytesWritten == recordLen) {
          String value = sb.toString();
          String key = value.substring(keyStartAt,keyEndAt+1);
          valMap.put(key,sb.toString());
          //LOG.info("CUSTOMKEY=" + key + 
          //        " VALUE=" +sb.toString());
          writer.write(sb.toString());
          sb = new StringBuffer();
          lastRecordStart = i+1;
          recordBytesWritten=0;
        }


      }
    } finally {
      writer.close();
    }

    return valMap;
  }

  @Test
  public void testSplitSizesWithDefaultKeys() throws Exception {
    Path file = new Path(workDir, "testSplitSizesWithDefaultKeys.txt");

    int seed = new Random().nextInt();
    LOG.info("seed = " + seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);

    // try 20 random tests of various record/size, total record combinations
    // to verify split rules
    int MAX_TESTS = 20;
    for (int i = 0; i < MAX_TESTS; i++) {

      LOG.info("----------------------------------------------------------");

      // max total records of 999
      int TOTAL_RECORDS = random.nextInt(999)+1; // avoid 0
      
      // max bytes in a record of 100K
      int RECORD_LENGTH = random.nextInt(1024*100)+1; // avoid 0
      
      // for the 11th test, force a record length of 1
      if (i == 10) {
        RECORD_LENGTH = 1;
      }

      // the total bytes in the test file
      int FILE_SIZE = (TOTAL_RECORDS * RECORD_LENGTH);


      LOG.info("TOTAL_RECORDS=" + TOTAL_RECORDS + 
          " RECORD_LENGTH=" +RECORD_LENGTH);

      // write the test file
      Map<Long,String> valMap = writeDefaultKeyDummyFile(file,FILE_SIZE,RECORD_LENGTH);

      // verify exists
      assertTrue(localFs.exists(file));


      // set the fixed length record length config property 
      Configuration testConf = new Configuration(defaultConf);
      testConf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, RECORD_LENGTH);

      // arbitrarily set the maxSplitSize, for the FIRST test use the default split size
      // then all subsequent tests pick a split size that is random. For the LAST test
      // lets test a split size that is LESS than the record length itself...
      if (i>0) {
        long maxSplitSize = 0;

        if (i == (MAX_TESTS-1)) {
          // test a split size that is less than record len
          maxSplitSize = (long)Math.floor(RECORD_LENGTH /2);

        } else if (i > 0) {

          if (MAX_TESTS % i == 0) {
            // lets create a split size that is forced to be 
            // smaller than the end file itself, (ensures 1+ splits)
            maxSplitSize = (FILE_SIZE - random.nextInt(FILE_SIZE));

          } else {
            // just pick a random split size with no upper bound 
            maxSplitSize = random.nextInt(Integer.MAX_VALUE);
          }

        }


        testConf.setLong("mapreduce.input.fileinputformat.split.maxsize", 
            maxSplitSize);
        LOG.info("max split size forced to :" +maxSplitSize);
      }


      // create the job, and setup the input path
      Job job = new Job(testConf);
      FileInputFormat.setInputPaths(job, workDir);


      // try splitting the file in a variety of sizes
      FixedLengthInputFormat format = new FixedLengthInputFormat();

      List<InputSplit> splits = format.getSplits(job);
      LOG.info("TOTAL SPLITS = " + splits.size());


      // test combined split lengths = total file size
      long totalSize = 0;
      long totalRecords = 0;
      for (InputSplit split : splits) {
        // total size
        totalSize+=split.getLength();

        // careate dummy context
        TaskAttemptContext context = 
          MapReduceTestUtil.createDummyMapTaskAttemptContext(job.getConfiguration());

        // get the record reader
        RecordReader<BytesWritable, BytesWritable> reader = 
          format.createRecordReader(split, context);
        MapContext<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mcontext = 
          new MapContextImpl<BytesWritable, BytesWritable, BytesWritable, BytesWritable>(
              job.getConfiguration(), context.getTaskAttemptID(), reader, 
              null, null, MapReduceTestUtil.createDummyReporter(), split);
        reader.initialize(split, mcontext);

        // verify the correct class
        Class<?> clazz = reader.getClass();
        assertEquals("RecordReader class is FixedLengthRecordReader.", 
            FixedLengthRecordReader.class, clazz);

        // plow through the records in this split
        while (reader.nextKeyValue()) {
          BytesWritable key = reader.getCurrentKey();
          byte[] value = reader.getCurrentValue().getBytes();
          String valueStr = new String(value);
          assertEquals("Record length is correct",RECORD_LENGTH,value.length);

          assertEquals("Record starts with char [!]",'!',valueStr.charAt(0));
          assertEquals("Record ends with char [!]",'!',valueStr.charAt(RECORD_LENGTH-1));

          byte[] keyBytes = key.getBytes();
          long longKey = toLong(keyBytes);
          //LOG.info("READ IN KEY = "+ longKey + " from " + keyBytes.length +" " +keyBytes);
          String origRecord = valMap.get(longKey);
          assertNotNull("Orig record found in map by key " +longKey,origRecord);
          assertEquals("Orig record, matches read-in record",
              origRecord,new String(value));

          totalRecords++;
        }

        reader.close();



      }
      assertEquals("Total original records = total read records",
          valMap.size(),totalRecords);
      assertEquals("Total length of combined splits is correct:",
          FILE_SIZE,totalSize);

    }
    
  }
  
  @Test
  public void testSplitSizesWithCustomKeys() throws Exception {
    Path file = new Path(workDir, "testSplitSizesWithCustomKeys.txt");

    int seed = new Random().nextInt();
    LOG.info("seed = " + seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);

    // try 20 random tests of various record/size, total record combinations
    // to verify split rules, + specify custom keys
    int MAX_TESTS = 20;
    for (int i = 0; i < MAX_TESTS; i++) {

      LOG.info("----------------------------------------------------------");

      // max total records of 99
      int TOTAL_RECORDS = random.nextInt(99)+1; // avoid 0
      
      // max bytes in a record of 25K
      int RECORD_LENGTH = random.nextInt(1024*25)+1; // avoid 0
      
      // custom key start (somewhere between 1 and length/2)
      int KEY_START_AT = (RECORD_LENGTH==1 ? 1 : random.nextInt(RECORD_LENGTH/2)+1);
      
      // custom key end at
      int KEY_END_AT = KEY_START_AT + random.nextInt(RECORD_LENGTH-KEY_START_AT);

      // the total bytes in the test file
      int FILE_SIZE = (TOTAL_RECORDS * RECORD_LENGTH);


      LOG.info("TOTAL_RECORDS=" + TOTAL_RECORDS + 
          " RECORD_LENGTH=" +RECORD_LENGTH +
          " KEY START=" + KEY_START_AT +
          " KEY END=" + KEY_END_AT);

      // write the test file
      Map<String,String> valMap = writeCustomKeyDummyFile(file,FILE_SIZE,RECORD_LENGTH,KEY_START_AT,KEY_END_AT);
      while(valMap.size() != TOTAL_RECORDS) {
    	  // if the length is not = total, then we have duplicate keys created, try again...
    	  valMap = writeCustomKeyDummyFile(file,FILE_SIZE,RECORD_LENGTH,KEY_START_AT,KEY_END_AT);
      }

      // verify exists
      assertTrue(localFs.exists(file));


      // set the fixed length record length config property 
      Configuration testConf = new Configuration(defaultConf);
      testConf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, RECORD_LENGTH);
      
      // set the key/start/end at
      testConf.setInt(FixedLengthInputFormat.FIXED_RECORD_KEY_START_AT, KEY_START_AT);
      testConf.setInt(FixedLengthInputFormat.FIXED_RECORD_KEY_END_AT, KEY_END_AT);

      // arbitrarily set the maxSplitSize, for the FIRST test use the default split size
      // then all subsequent tests pick a split size that is random. For the LAST test
      // lets test a split size that is LESS than the record length itself...
      if (i>0) {
        long maxSplitSize = 0;

        if (i == (MAX_TESTS-1)) {
          // test a split size that is less than record len
          maxSplitSize = (long)Math.floor(RECORD_LENGTH /2);

        } else if (i > 0) {

          if (MAX_TESTS % i == 0) {
            // lets create a split size that is forced to be 
            // smaller than the end file itself, (ensures 1+ splits)
            maxSplitSize = (FILE_SIZE - random.nextInt(FILE_SIZE));

          } else {
            // just pick a random split size with no upper bound 
            maxSplitSize = random.nextInt(Integer.MAX_VALUE);
          }

        }


        testConf.setLong("mapreduce.input.fileinputformat.split.maxsize", 
            maxSplitSize);
        LOG.info("max split size forced to :" +maxSplitSize);
      }


      // create the job, and setup the input path
      Job job = new Job(testConf);
      FileInputFormat.setInputPaths(job, workDir);


      // try splitting the file in a variety of sizes
      FixedLengthInputFormat format = new FixedLengthInputFormat();

      List<InputSplit> splits = format.getSplits(job);
      LOG.info("TOTAL SPLITS = " + splits.size());


      // test combined split lengths = total file size
      long totalSize = 0;
      long totalRecords = 0;
      for (InputSplit split : splits) {
        // total size
        totalSize+=split.getLength();

        // careate dummy context
        TaskAttemptContext context = 
          MapReduceTestUtil.createDummyMapTaskAttemptContext(job.getConfiguration());

        // get the record reader
        RecordReader<BytesWritable, BytesWritable> reader = 
          format.createRecordReader(split, context);
        MapContext<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mcontext = 
          new MapContextImpl<BytesWritable, BytesWritable, BytesWritable, BytesWritable>(
              job.getConfiguration(), context.getTaskAttemptID(), reader, 
              null, null, MapReduceTestUtil.createDummyReporter(), split);
        reader.initialize(split, mcontext);

        // verify the correct class
        Class<?> clazz = reader.getClass();
        assertEquals("RecordReader class is FixedLengthRecordReader.", 
            FixedLengthRecordReader.class, clazz);

        // plow through the records in this split
        while (reader.nextKeyValue()) {
          BytesWritable key = reader.getCurrentKey();
          byte[] value = reader.getCurrentValue().getBytes();
          String valueStr = new String(value);
          assertEquals("Record length is correct",RECORD_LENGTH,value.length);


          byte[] keyBytes = key.getBytes();
          String keyStr = new String(keyBytes);
          //LOG.info("READ IN KEY = "+ longKey + " from " + keyBytes.length +" " +keyBytes);
          String origRecord = valMap.get(keyStr);
          assertNotNull("Orig record found in map by key " +keyStr,origRecord);
          assertEquals("Orig record, matches read-in record",
              origRecord,new String(value));

          totalRecords++;
        }

        reader.close();



      }
      assertEquals("Total original records = total read records",
          valMap.size(),totalRecords);
      assertEquals("Total length of combined splits is correct:",
          FILE_SIZE,totalSize);

    }
    
  }
  
  private long toLong(byte[] bytes) {
	  int offset =0;
	  int length = (Long.SIZE/Byte.SIZE);
	  int SIZEOFLONG = (Long.SIZE/Byte.SIZE);
	  
      if (bytes == null || length !=  SIZEOFLONG ||
        (offset + length > bytes.length)) {
        return -1L;
      }
      long l = 0;
      for(int i = offset; i < (offset + length); i++) {
        l <<= 8;
        l ^= (long)bytes[i] & 0xFF;
      }
      return l;
    } 

}
