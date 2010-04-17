package com.bixolabs.cascading.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class AvroSchemeTest {

    private static final String OUTPUT_DIR = "build/test/AvroSchmeTest/"; 
    
    @Before
    public void setup() throws IOException {
        File outputDir = new File(OUTPUT_DIR);
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    
//    @Test
//    public void testAvroSchemaGeneration() {
//        AvroScheme avroScheme = new AvroScheme(new Fields("a", "b", "c"), 
//                        new Class[] { List.class, Long.class, Integer.class, BytesWritable.class});
//        Schema schema = avroScheme.getSchema();
//        System.out.println(schema.toString());
//    }

    @Test
    public void testSchemeChecks() {
 
        try {
            new AvroScheme(new Fields("a", "b"), new Class[] { String.class, Long.class });
        } catch (Exception e) {
            fail("Exception should not be thrown - this is the valid case");
        }

        try {
            new AvroScheme(new Fields(), new Class[] {});
            fail("Exception should be thrown when scheme field is empty");
        } catch (Exception e) {
        }

        try {
            new AvroScheme(new Fields("a", "b", "c"), new Class[] { Integer.class });
            fail("Exception should be thrown as there are more fields defined than types");
        } catch (Exception e) {
        }

        try {
            new AvroScheme(new Fields("a"), new Class[] { Integer.class, String.class });
            fail("Exception should be thrown as there are more types defined than fields");
        } catch (Exception e) {
        }

        try {
            new AvroScheme(new Fields("array"), new Class[] { List.class, Long.class });
        } catch (Exception e) {
            fail("Exception shouldn't be thrown as array type is valid");
        }

        try {
            new AvroScheme(new Fields("array"), new Class[] { List.class, List.class });
            fail("Exception should be thrown as array type isn't a primitive");
        } catch (Exception e) {
        }

    }

    
    @Test
    public void testRoundTrip() throws Exception {
        final Fields testFields = new Fields("arrayOfLongsField", "stringField", "mapOfStringsField", "longField");
        final Class[] schemeTypes = {List.class, Long.class, String.class, Map.class, String.class, Long.class};
       final String in = OUTPUT_DIR+ "testRoundTrip/in";
        final String out = OUTPUT_DIR + "testRoundTrip/out";
        final String verifyout = OUTPUT_DIR + "testRoundTrip/verifyout";
        final int numRecords = 11; // 0...10
        
        // Read from Sequence file and write to AVRO
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        makeSourceTuples(lfsSource, numRecords);
        Pipe importPipe = new Pipe("import");

        Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
        Flow flow = new FlowConnector().connect(lfsSource, avroSink, importPipe);
        flow.complete();
        
        // Not exactly round-trip since we are reading from AVRO and writing to TextLine
        // (mainly because it can also be manually inspected).
        Tap avroSource = new Lfs(new AvroScheme(testFields, schemeTypes), out);
        Pipe readPipe = new Pipe("import");
        Tap verifySink = new Hfs(new SequenceFile(testFields), verifyout, SinkMode.REPLACE);

        Flow readFlow = new FlowConnector().connect(avroSource, verifySink, readPipe);
        readFlow.complete();

        TupleEntryIterator sinkTuples = verifySink.openForRead(new JobConf());
        assertTrue(sinkTuples.hasNext());
        
        int i = 0;
        while (sinkTuples.hasNext()){
            TupleEntry t = sinkTuples.next();
            assertEquals(makeStringValue("stringValue", i), t.getString("stringField"));
            assertEquals(i, t.getLong("longField"));
            i++;
        }
        assertEquals(numRecords, i);
    }


    private String makeStringValue(String str, int i) {
        return str + "-" + i;
    }
   

    private void makeSourceTuples(Lfs lfs, int numTuples) throws IOException {
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        // Tuple -> Array<Long> String, Map<String> Long
        for (int i = 0; i < numTuples; i++) {
            Tuple t = new Tuple();
            Tuple arrTuple = new Tuple();
            arrTuple.addAll(new Long(60 + i), new Long(70 + i));
            t.add(arrTuple);
            t.add(makeStringValue("stringValue", i));
            Tuple mapTuple = new Tuple();
            mapTuple.addAll("mapVal1-" + i, "mapVal2-" + i);
            t.add(mapTuple);
            Long longVal = new Long(i);
            t.add(longVal);
            write.add(t);
        }
        write.close();
    }

 }
