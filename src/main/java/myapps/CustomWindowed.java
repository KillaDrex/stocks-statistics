package myapps;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.LocalDateTime;
import java.time.Instant;

import java.util.TimeZone;

public class CustomWindowed<K> extends Windowed<K> {
	public CustomWindowed(K key, Window window) {
		super(key, window);
	}

	@Override
	public String toString() {
		// get window
		Window window = this.window();

		LocalDateTime ldtS = LocalDateTime.ofInstant(Instant.ofEpochMilli(window.start() ), TimeZone.getTimeZone("Singapore").toZoneId() );
        LocalDateTime ldtE = LocalDateTime.ofInstant(Instant.ofEpochMilli(window.end() ), TimeZone.getTimeZone("Singapore").toZoneId() );
        return "[" + ldtS.toString().split("T")[1] + "-" + ldtE.toString().split("T")[1] + "]->" + this.key(); // [19:13:40-19:13:45]->DZE   
	}
}