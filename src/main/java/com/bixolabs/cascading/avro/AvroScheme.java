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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;


import cascading.scheme.Scheme;
import cascading.tap.Tap;
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
    private String _jsonSchema;
    private Class _specificRecordClass = null;

    public AvroScheme(Fields schemeFields, Class specificRecordClass, String jsonSchema) {
        super(schemeFields, schemeFields);
        _specificRecordClass  = specificRecordClass;
        throw new UnsupportedOperationException("This isn't currently working");
    }
    
    public AvroScheme(Fields schemeFields, String jsonSchema) {
        super(schemeFields, schemeFields);
        
        // TODO - only allow records in the schema ?
        Schema schema = Schema.parse(jsonSchema);
        if (schema.getFields().size() < schemeFields.size()) {
            throw new IllegalArgumentException("There are more scheme fields than those defined in the AVRO schema");
        }
        _jsonSchema = jsonSchema;

        if (schemeFields.size() == 0) {
            throw new IllegalArgumentException("There must be at least one field");
        }
        
        _schemeFields = schemeFields;
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        
        if (_specificRecordClass != null) {
            AvroJob.setOutputSpecific(conf, _jsonSchema);
        } else {
            AvroJob.setOutputGeneric(conf, _jsonSchema);
        }
        LOGGER.info(String.format("Initializing Avro scheme for sink tap - scheme fields: %s", _schemeFields));
    }

    public void sourceInit(Tap tap, JobConf conf) throws IOException {

        if (_specificRecordClass != null) {
            AvroJob.setInputSpecific(conf, _jsonSchema);
        } else {
            AvroJob.setInputGeneric(conf, _jsonSchema);
        }
        LOGGER.info(String.format("Initializing Avro scheme for source tap - scheme fields: %s", _schemeFields));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        // Create the appropriate AvroWrapper<T> from the result, and pass that as the key for the collect
        Fields sinkFields = getSinkFields();
        Tuple result = sinkFields != null ? tupleEntry.selectTuple(sinkFields) : tupleEntry.getTuple();

        if (_specificRecordClass != null) {
            // TODO Figure out how to pass in a specific record
        } else {
            // Create a Generic data using the sink field names
            GenericData.Record datum = new GenericData.Record(Schema.parse(_jsonSchema));
            for (int i = 0; i < sinkFields.size(); i++) {
                datum.put(sinkFields.get(i).toString(), result.get(i));
            }
            AvroWrapper<GenericData.Record> wrapper = new AvroWrapper<GenericData.Record>(datum) ;
            outputCollector.collect(wrapper, NullWritable.get());
       }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple source(Object key, Object ignore) {
        // Convert the AvroWrapper<T> key value to a tuple.
       Tuple result = new Tuple();

        Fields sourceFields = getSourceFields();
        if (_specificRecordClass != null) {
            // TODO there's just one object of the specific kind so just pass that along
        } else {
            // Unpack this datum into source tuple fields
            AvroWrapper<GenericData.Record> wrapper = (AvroWrapper<GenericData.Record>)key;
            GenericData.Record datum = wrapper.datum();
            for (int i = 0; i < sourceFields.size(); i++) {
                result.add(datum.get(sourceFields.get(i).toString()));
            }
        }
        return result;
    }

    // TODO - add hashcode and equals, once we have the fields settled.

}
