/**
 * Copyright 2010 TransPac Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Based on cascading.jdbc code released into the public domain by
 * Concurrent, Inc.
 */

package com.bixolabs.cascading.avro;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * The AvroScheme class is a {@link Scheme} subclass. It supports reading and writing of data
 * that has been serialized using Apache Avro.
 */
@SuppressWarnings("serial")
public class AvroScheme extends Scheme {
    private static final Logger LOGGER = Logger.getLogger(AvroScheme.class);

    private Fields _schemeFields;
    private Class[] _schemeTypes;
    
    public AvroScheme(Fields schemeFields, Class[] schemeTypes) {
        super(schemeFields, schemeFields);
        
        if (schemeFields.size() == 0) {
            throw new IllegalArgumentException("There must be at least one field");
        }
        
        if (schemeTypes.length != schemeFields.size()) {
            throw new IllegalArgumentException("You must have a schemeType for every field");
        }

        _schemeFields = schemeFields;
        _schemeTypes = schemeTypes;
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        
        // TODO - make it so. We need to build an Avro schema from the _schemeFields & _schemeTypes,
        // and use that to configure it properly.
        
        LOGGER.info(String.format("Initializing Avro scheme for sink tap - scheme fields: %s and scheme classes: %s", _schemeFields, _schemeTypes));
    }

    public void sourceInit(Tap tap, JobConf conf) throws IOException {

        // TODO - make it so. We need to build an Avro schema from the _schemeFields & _schemeTypes,
        // and use that to configure it properly.
        
        LOGGER.info(String.format("Initializing Avro scheme for source tap - scheme fields: %s and scheme classes: %s", _schemeFields, _schemeTypes));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        Tuple result = getSinkFields() != null ? tupleEntry.selectTuple(getSinkFields()) : tupleEntry.getTuple();

        // TODO - create the appropriate AvroWrapper<T> from the result, and pass that as the key for the collect
        
        outputCollector.collect(null, NullWritable.get());
    }

    @Override
    public Tuple source(Object key, Object value) {
        // TODO - convert the AvroWrapper<T> key value to a tuple.
        
        return (Tuple)null;
    }

    // TODO - add hashcode and equals, once we have the fields settled.

}
