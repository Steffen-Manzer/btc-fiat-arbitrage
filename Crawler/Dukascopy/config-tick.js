// Einstellungen
module.exports = {

	// Alle sinnvollen Jahre abfragen
	yearFrom: 2010,
	
	// Bis heute abfragen
	yearTo: (new Date()).getFullYear(),
	
	// Speicherort
	targetDirectory: "../../Daten/dukascopy/",
	
	// Währungspaare
	// Siehe https://www.npmjs.com/package/dukascopy-node, Abschnitt "Supported Instruments"
	pairs: ["audusd", "eurusd", "gbpusd", "usdcad", "usdchf", "usdjpy", "nzdusd"],
	
	// Wie viele Paare in einem Durchlauf? (0 = Unbegrenzt)
	maxNumDatasetsPerIteration: 0
	
};
