package com.tapiax.metrics.commons;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.bson.Document;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

/*
 * This classDeserialize  from XML to Mongo  Document
 * */

public class XmlDeserializer implements Deserializer<Document> {

	private XmlMapper xmlMapper = new XmlMapper();

	@Override
	public Document deserialize(String topic, byte[] data) {
		
		Document document = null;
		try {
			document = xmlMapper.readValue(data, Document.class);
		} catch (Exception e) {
			 throw new SerializationException(e);
		}
		return document;
	}

}
