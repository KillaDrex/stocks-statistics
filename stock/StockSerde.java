package dre.local.stock;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.Map;

public class StockSerde implements Serde<dre.local.stock.Stock>, Serializer<dre.local.stock.Stock>, Deserializer<dre.local.stock.Stock> {
	@Override
	public void close() {}
	@Override 
	public void configure(Map<String, ?> configs, boolean isKey) {}

	@Override 
	public Deserializer<dre.local.stock.Stock> deserializer() {
		return this;
	}

	@Override
	public Serializer<dre.local.stock.Stock> serializer() {
		return this;
	}

	@Override
	public byte[] serialize(String topic, dre.local.stock.Stock data) {
		// null data
		if (data == null)
			return null;

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
			 ObjectOutputStream oos = new ObjectOutputStream(bos) ) {
				// write object to stream
				oos.writeObject(data);

				// get bytes of stream
				return bos.toByteArray();
		} catch (Exception e) {
			throw new SerializationException("Error in serializing stock record", e);
		}
	}

	@Override
	public dre.local.stock.Stock deserialize(String topic, byte[] data) {
		// null data
		if (data == null)
			return null;

		try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
			 ObjectInputStream ois = new ObjectInputStream(bis) ) {
				// deserialize object
				dre.local.stock.Stock stock = (dre.local.stock.Stock)ois.readObject();

				// return object
				return stock;
		} catch (Exception e) {
			throw new SerializationException("Error in deserializing stock record", e);
		}				
	}
}