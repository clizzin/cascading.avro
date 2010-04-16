package com.bixolabs.cascading.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.metrics.spi.NullContext;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
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
    private static final String SCHEMA_JSON = 
                    "{\"type\": \"record\", " 
                    + "\"name\": \"Test\", " 
                    + "\"fields\": [" 
                    + "  {\"name\":\"stringField\", \"type\":\"string\"},"
                    + "  {\"name\":\"longField\", \"type\":\"long\"}" 
                    + "  ]" 
                    + "}";

    private static final String OUTPUT_DIR = "build/test/AvroSchmeTest/"; 
    
    @Before
    public void setup() throws IOException {
        File outputDir = new File(OUTPUT_DIR);
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    
    @Test
    public void testAvroSchemaGeneration() {
        // TODO
    }

    @Test
    public void testSchemeChecks() {
 
        try {
            new AvroScheme(new Fields("a", "b"), SCHEMA_JSON);
        } catch (Exception e) {
            fail("Exception should not be thrown - this is the valid case");
        }

        try {
            new AvroScheme(new Fields(), SCHEMA_JSON);
            fail("Exception should be thrown when scheme field is empty");
        } catch (Exception e) {
            // valid
        }

        try {
            new AvroScheme(new Fields("a"), SCHEMA_JSON);
        } catch (Exception e) {
            fail("Exception should not be thrown - as it is okay to have less fields");
        }

        try {
            new AvroScheme(new Fields("a", "b", "c"), SCHEMA_JSON);
            fail("Exception should be thrown as there are more fields defined than present in the JSON schema");
        } catch (Exception e) {
        }
    }

    @Test
    public void testRoundTrip() throws Exception {
        final Fields testFields = new Fields("stringField", "longField");
        final String in = OUTPUT_DIR+ "testRoundTrip/in";
        final String out = OUTPUT_DIR + "testRoundTrip/out";
        final String textout = OUTPUT_DIR + "testRoundTrip/textout";
        final int numRecords = 11; // 0...10
        
        // Read from Sequence file and write to AVRO
        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        makeSourceTuples(lfsSource, numRecords);
        Pipe importPipe = new Each("import", new Identity());
        importPipe = new Each(importPipe, testFields, new ConvertSeqToAvro());

        Tap avroSink = new Lfs(new AvroScheme(testFields, SCHEMA_JSON), out);
        Flow flow = new FlowConnector().connect(lfsSource, avroSink, importPipe);
        flow.complete();
        
        // Not exactly round-trip since we are reading from AVRO and writing to TextLine
        // (mainly because it can also be manually inspected).
        Tap avroSource = new Lfs(new AvroScheme(testFields, SCHEMA_JSON), out);
        Pipe readPipe = new Each("import", new Identity());
        readPipe = new Each(readPipe, testFields, new ConvertAvroToTextLine());
        Tap textSink = new Hfs(new TextLine(new Fields("line")), textout, SinkMode.REPLACE);

        Flow readFlow = new FlowConnector().connect(avroSource, textSink, readPipe);
        readFlow.complete();

        TupleEntryIterator sinkTuples = textSink.openForRead(new JobConf());
        assertTrue(sinkTuples.hasNext());
        
        int i = 0;
        while (sinkTuples.hasNext()){
            TupleEntry t = sinkTuples.next();
            String expected = makeStringValue("stringValue", i);
            assertEquals(expected, t.get(0));
            i++;
        }
        assertEquals(numRecords, i);
    }


    private String makeStringValue(String str, int i) {
        return str + "-" + i + "\t" + Integer.toString(i);
    }
   
    

    private void makeSourceTuples(Lfs lfs, int numTuples) throws IOException {
        TupleEntryCollector write = lfs.openForWrite(new JobConf());
        // Tuple -> String, Long
        for (int i = 0; i < numTuples; i++) {
            Tuple t = new Tuple();
            t.add(makeStringValue("stringValue", i));
            long longVal = i;
            t.add(longVal);
            write.add(t);
        }
        write.close();
    }

    @SuppressWarnings("serial")
    private static class ConvertSeqToAvro extends BaseOperation<NullContext> implements Function<NullContext> {

        public ConvertSeqToAvro() throws IOException {
            super(new Fields("stringField", "longField"));
        }

        @Override
        public void operate(FlowProcess arg0, FunctionCall<NullContext> funcCall) {
            Tuple inTuple = funcCall.getArguments().getTuple();
            Tuple convertedDatum = new Tuple();
            Utf8 utfStr = new Utf8((String)inTuple.getObject(0));
            convertedDatum.add(utfStr);
            convertedDatum.add(inTuple.getObject(1));
 
            funcCall.getOutputCollector().add(convertedDatum);
        }

    }
    
    @SuppressWarnings("serial")
    private static class ConvertAvroToTextLine extends BaseOperation<NullContext> implements Function<NullContext> {

        public ConvertAvroToTextLine() throws IOException {
            super(new Fields("line"));
        }

        @Override
        public void operate(FlowProcess arg0, FunctionCall<NullContext> funcCall) {
            Tuple inTuple = funcCall.getArguments().getTuple();
            Tuple convertedDatum = new Tuple();
           
            Utf8 utfStr = (Utf8)inTuple.getObject(0);
            String convertedStr = utfStr.toString() + "\t" + inTuple.getObject(1);
            convertedDatum.add(convertedStr);
            funcCall.getOutputCollector().add(convertedDatum);
        }

    }

}
