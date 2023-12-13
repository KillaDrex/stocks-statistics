package myapps;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.common.utils.Bytes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.KeyValueStore;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Suppressed;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Instant;

import java.util.TimeZone;
import java.util.Arrays;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import dre.local.stock.Stock;
import dre.local.stock.StockSerde;

public class StocksStatistics {
	//  window size (in seconds)
	private static final int WINDOW_SIZE = 60;

	public static void main(String args[]) {
		// set kafka app properties
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stocks_statistics");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass() );
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass() );
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

		// topology
		final StreamsBuilder builder = new StreamsBuilder();

		// create streams to separate trade data
		Map<String, KStream<String, String>> stocksData = builder.stream("stock-trades", Consumed.with(Serdes.String(), Serdes.String() ).withName("stock_trades"))
			.flatMapValues(new ValueMapper<String, Iterable<String> >() {
				@Override
				public Iterable<String> apply(String value) {
					// get record values
					String[] values = value.split(";");

					// update first record value
					values[0] = "";

					// add record
					String[] recordValues = {values[0], values[1] + "Min", values[1] + "Max", values[2]};

					return Arrays.asList(recordValues);
				}
			}, Named.as("separate_trade_data") )
			.split(Named.as("split_records_into_streams-") )
			.branch(new Predicate<String, String>() {
				@Override
				public boolean test(String tickerSymbol, String tradeData) {
					// for trade count
					if (tradeData.equals("") )
						return true;
					return false;
				}
			}, Branched.as("stock_trade_count") )
			.branch(new Predicate<String, String>() {
				@Override
				public boolean test(String tickerSymbol, String tradeData) {
					// for min price
					if (tradeData.indexOf("Min") != -1)
						return true;
					return false;
				}
			}, Branched.as("stock_trade_min_price") )
			.branch(new Predicate<String, String>() {
				@Override
				public boolean test(String tickerSymbol, String tradeData) {
					// for min price
					if (tradeData.indexOf("Max") != -1)
						return true;
					return false;
				}
			}, Branched.as("stock_trade_max_price") )			
			.defaultBranch(Branched.as("stock_trade_shares_count"));

		// aggregators
		Aggregator<String, String, Stock> tradeCountAgg = new Aggregator<String, String, Stock>() {
			@Override
			public Stock apply(String tickerSymbol, String value, Stock stock) {
				// newly-initialized aggregation value
				if (stock.getTrades() == null) 
					stock.setTrades(0l);

				// increment trades
				stock.setTrades(stock.getTrades() + 1);

				return stock;
			}
		};
		Aggregator<String, String, Stock> tradeMinPriceAgg = new Aggregator<String, String, Stock>() {
			@Override
			public Stock apply(String tickerSymbol, String currentPrice, Stock stock) {
				// strip current price
				currentPrice = currentPrice.split("Min")[0];

				// newly-initialized aggregation value
				if (stock.getMinPrice() == null) {
					stock.setMinPrice(Long.parseLong(currentPrice) );
					return stock;
				}

				if (Long.parseLong(currentPrice) < stock.getMinPrice() ) {
					// current price is lower than recorded min price
					stock.setMinPrice(Long.parseLong(currentPrice) );
				} 

				return stock;
			}
		};
		Aggregator<String, String, Stock> tradeMaxPriceAgg = new Aggregator<String, String, Stock>() {
			@Override
			public Stock apply(String tickerSymbol, String currentPrice, Stock stock) {
				// strip current price
				currentPrice = currentPrice.split("Max")[0];

				// newly-initialized aggregation value
				if (stock.getMaxPrice() == null) {
					stock.setMaxPrice(Long.parseLong(currentPrice) );
					return stock;
				}

				if (Long.parseLong(currentPrice) > stock.getMaxPrice() ) {
					// current price is higher than recorded max price
					stock.setMaxPrice(Long.parseLong(currentPrice) );
				} 

				return stock;
			}
		};
		Aggregator<String, String, Stock> tradeSharesCountAgg = new Aggregator<String, String, Stock>() {
			@Override
			public Stock apply(String tickerSymbol, String shares, Stock stock) {
				// newly-initialized aggregation value
				if (stock.getVolume() == null) 
					stock.setVolume(0l);

				// increment volume
				stock.setVolume(stock.getVolume() + Long.parseLong(shares) );

				return stock;
			}
		};
		Initializer<Stock> init = new Initializer<Stock>() {
			@Override
			public Stock apply() {
				return new Stock();
			}
		};

		// aggregate windowed records (emit only final window result)
		KTable<Windowed<String>, Stock> stockStatistics = stocksData.get("split_records_into_streams-stock_trade_count")
			.groupByKey().cogroup(tradeCountAgg)
			.cogroup(stocksData.get("split_records_into_streams-stock_trade_min_price").groupByKey(), tradeMinPriceAgg)
			.cogroup(stocksData.get("split_records_into_streams-stock_trade_max_price").groupByKey(), tradeMaxPriceAgg)
			.cogroup(stocksData.get("split_records_into_streams-stock_trade_shares_count").groupByKey(), tradeSharesCountAgg)
			.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(WINDOW_SIZE) ) )
			.aggregate(init, Named.as("stock_statistics_aggregation"), Materialized.<String, Stock, WindowStore<Bytes, byte[]>>as("stock_statistics").withValueSerde(new StockSerde() ).withKeySerde(Serdes.String() ) )
			.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded() ).withName("emit_only_final_window_result") );

		// create windowed serde
		final Serde<Windowed<String>> WINDOWED_SERDE = WindowedSerdes.timeWindowedSerdeFrom(String.class, WINDOW_SIZE*1000);

		// materialize final stream
		stockStatistics
			.toStream(Named.as("window_results_table_to_stream"))
			.selectKey(new KeyValueMapper<Windowed<String>, Stock, String>() {
				@Override
				public String apply(Windowed<String> tickerSymbol, Stock stock) {
					// replace keys with CustomWindowed key class
					return new CustomWindowed<String>(tickerSymbol.key(), tickerSymbol.window() ).toString();
				}
			})
			.to("stockstats-output", 
				Produced.with(Serdes.String(), new StockSerde() ).withName("stock_statistics") );
		// stockStatistics.toStream(new KeyValueMapper<Windowed<String>, Stock, Windowed<String>>() {
		// 	@Override
		// 	public Windowed<String> apply(Windowed<String> windowed, Stock stock) {
		// 		return new CustomWindowed(windowed.key(), windowed.window() );
		// 	}
		// })
		// 	.print(Printed.toSysOut() );

		Topology topology = builder.build();

		// create app instance
		KafkaStreams streams = new KafkaStreams(topology, props);

		// setup shutdown hook
		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}