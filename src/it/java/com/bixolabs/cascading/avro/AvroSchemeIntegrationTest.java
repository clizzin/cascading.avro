package com.bixolabs.cascading.avro;

import static org.junit.Assert.fail;

import org.junit.Test;

import cascading.tuple.Fields;


public class AvroSchemeIntegrationTest {

    @Test
    public void testSchemeChecks() {
        try {
            new AvroScheme(new Fields(), new Class[0]);
            fail("Exception should be thrown when scheme field is empty");
        } catch (Exception e) {
            // valid
        }

        try {
            new AvroScheme(new Fields("a", "b", "c"), new Class[0]);
            fail("Exception should be thrown when there isn't one class per field");
        } catch (Exception e) {
            // valid
        }

        try {
            new AvroScheme(new Fields("a"), new Class[] { Integer.class });
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }
    }

    @Test
    public void testRoundTrip() throws Exception {
        
    }
}
