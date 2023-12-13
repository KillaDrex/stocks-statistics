package dre.local.stock;

import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;

import dre.local.stock.Stock;

import java.util.Map;

public class TimeWindowedStockSymbolDeserializer extends TimeWindowedDeserializer<String> {
	public TimeWindowedStockSymbolDeserializer() {
		// call super constructor
		super(Serdes.String().deserializer(), 5000l);
	}

	// @Override
	// public void close() {}

	// @Override
	// public void configure(Map<String, ?> configs, boolean isKey) {}

	// @Override
	// public Windowed<String> deserialize(String topic, byte[] data) {
	// 	// null data
	// 	if (data == null)
	// 		return null;

	// 	try {
	// 		final Serde<Windowed<String>> WINDOWED_SERDE = WindowedSerdes.timeWindowedSerdeFrom(String.class, 5000);

	// 	} catch (Exception e) {
	// 		throw new SerializationException("Error in deserializing key", e);
	// 	}
	// }

	// @Override
	// public Long getWindowSize() {
	// 	return 5000l;
	// }
}