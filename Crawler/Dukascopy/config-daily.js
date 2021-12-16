// Einstellungen
module.exports = {

	// Frühester Datensatz je nach Paar
// 	dateFrom: {
// 		audusd: "1993-01-04",
// 		eurusd: "1973-03-01",
// 		gbpusd: "1986-02-10",
// 		nzdusd: "1991-07-08",
// 		usdcad: "1986-02-10",
// 		usdchf: "1986-02-10",
// 		usdjpy: "1986-02-10"
// 	},
	dateFrom: "1993-01-04",
	
	// Letzter Datensatz
	dateTo: ((d => new Date(d.setDate(d.getDate()-1)))(new Date)).toISOString().substr(0, 10), // Gestern
	
	// Speicherort
	targetDirectory: "../../Daten/dukascopy/",
	
	// Währungspaare
	// Siehe https://www.npmjs.com/package/dukascopy-node, Abschnitt "Supported Instruments"
	pairs: ["audusd", "eurusd", "gbpusd", "usdcad", "usdchf", "usdjpy", "nzdusd"],
	
	// Wie viele Paare in einem Durchlauf? (0 = Unbegrenzt)
	maxNumDatasetsPerIteration: 0
	
};
