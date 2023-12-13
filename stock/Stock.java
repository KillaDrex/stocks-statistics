package dre.local.stock;

import java.io.Serializable;

public class Stock implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private Long trades, minPrice, maxPrice, volume;

	public void setTrades(Long trades) {
		this.trades = trades;
	}

	public Long getTrades() {
		return trades;
	}

	public void setMinPrice(Long minPrice) {
		this.minPrice = minPrice;
	}

	public Long getMinPrice() {
		return minPrice;
	}

	public void setMaxPrice(Long maxPrice) {
		this.maxPrice = maxPrice;
	}

	public Long getMaxPrice() {
		return maxPrice;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

	public Long getVolume() {
		return volume;
	}	

	@Override
	public String toString() {
		// set initial values
		String trades, minPrice, maxPrice, volume;

		// if null
		if (this.trades == null)
			trades = "";
		else trades = Long.toString(this.trades);
		if (this.minPrice == null)
			minPrice = "";
		else minPrice = Long.toString(this.minPrice);
		if (this.maxPrice == null)
			maxPrice = "";
		else maxPrice = Long.toString(this.maxPrice);
		if (this.volume == null)
			volume = "";
		else volume = Long.toString(this.volume);

		if (this.trades > 1)
			return String.format("[%s Trades, Min Price: %sPHP, Max Price: %sPHP, Volume: %s Shares]",
								trades, minPrice, maxPrice, volume);
		else 
			return String.format("[%s Trade, Min Price: %sPHP, Max Price: %sPHP, Volume: %s Shares]",
								trades, minPrice, maxPrice, volume);			
	}
}