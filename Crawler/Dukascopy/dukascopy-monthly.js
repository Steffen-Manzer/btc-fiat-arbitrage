// https://www.npmjs.com/package/dukascopy-node
const { getHistoricRates } = require('dukascopy-node');
const fs = require('fs');
const { exec, execSync } = require('child_process');

// Einstellungen
const config = require('./config-monthly.js');
const now = new Date();

// SIGINT (Ctrl+C) abfangen und nach aktuellem Task abbrechen
let sigintCaptured = false;
process.on('SIGINT', () => {
	
	if (sigintCaptured) {
		console.warn(`\n*** Interrupt erneut empfangen, harter Abbruch.`);
		process.exit();
	}
	
	console.warn(`\n*** Interrupt empfangen, beende aktuelle Abfrage...`);
	sigintCaptured = true;
});

// Abfrage starten
(async () => {
	
	config.targetDirectory = fs.realpathSync(config.targetDirectory) + "/";
	
	console.log(`Konfiguration:`);
	console.log(`    Wechselkurspaare: ${config.pairs.join(", ")}`);
	console.log(`    Datenpfad: ${config.targetDirectory}\n`);
	
	let numDatasetsProcessed = 0;
	let gzipAsyncCalled = false;
	for (let pair of config.pairs) {
		
		// Zielverzeichnis erstellen, falls es noch nicht existiert
		if (!fs.existsSync(`${config.targetDirectory}${pair.toUpperCase()}`)) {
			fs.mkdirSync(`${config.targetDirectory}${pair.toUpperCase()}`);
		}
		
		// Zeitmessung
		let tStart = new Date();
		
		// Ziel-CSV
		let targetFile = `${config.targetDirectory}${pair.toUpperCase()}/${pair.toUpperCase()}-monthly-bid.csv`;
		
		let dateFrom;
		if (typeof config.dateFrom === "object") {
			dateFrom = config.dateFrom[pair];
			if (dateFrom === undefined) {
				throw new TypeError(`dateFrom fehlt für ${pair}.`);
			}
		} else if (typeof config.dateFrom === "string") {
			dateFrom = config.dateFrom;
		} else {
			throw new TypeError("dateFrom ist falsch konfiguriert.");
		}
		
		// In Konsole ohne neue Zeile am Ende schreiben
		process.stdout.write(`Lade ${pair.toUpperCase()} bid ${dateFrom} - ${config.dateTo}... `);
		
		if (fs.existsSync(`${targetFile}.gz`)) {
			fs.unlinkSync(`${targetFile}.gz`);
		}
		
		// Dukascopy anfragen
		let data = await getHistoricRates({
			instrument: pair,
			dates: {
				from: dateFrom,
				to: config.dateTo 
			},
			timeframe: 'mn1',
			//priceType: 'bid', // bid, ask
			format: 'csv'
		});
		
		process.stdout.write("CSV... ");
		
		// Rohdaten in .csv schreiben
		fs.writeFileSync(targetFile, data);
		if (fs.statSync(targetFile).size === 0) {
			console.error(`\n*** Keine Daten geschrieben!`);
			continue;
		}
		
		// Wenn möglich, asynchron komprimieren
		process.stdout.write("gzip... ");
		if (sigintCaptured) {
			execSync(`/usr/bin/gzip '${targetFile}'`);
		} else {
			gzipAsyncCalled = true;
			exec(`/usr/bin/gzip '${targetFile}'`, (error, stdout, stderr) => {
				if (error) {
					console.error(`\n*** Fehler: ${error.message}`);
				}
				if (stderr) {
					console.error(`\n*** stderr: ${stderr}`);
				}
				if (stdout) {
					console.log(`\nstdout: ${stdout}`);
				}
			});
		}
		
		let duration = Math.round(((new Date()) - tStart) / 1000);
		console.log(`Fertig. ${duration}s vergangen.`);
		
		// Interrupt abfangen und nach aktueller Aufgabe abbrechen
		if (sigintCaptured) {
			console.log("Aktuelle Aufgabe abgeschlossen. Interrupt empfangen. Abbruch.");
			process.exit();
		}
		
		// Abbruch, wenn maximale Anzahl Datensätze erreicht
		numDatasetsProcessed++;
		if (config.maxNumDatasetsPerIteration > 0 && numDatasetsProcessed >= config.maxNumDatasetsPerIteration) {
			console.log("Maximale Anzahl an Datensätzen für einen Durchlauf erreicht. Abbruch.");
			process.exit();
		}
		
	} // Wechselkurse
	
	console.log("Alle Paare abgeschlossen.");
	
	if (gzipAsyncCalled) {
		console.log("Warte 10s, um verbleibende gzip-Operationen abzuschließen.");	
		await new Promise(resolve => setTimeout(resolve, 10000));
	}
	
})(); // Hauptfunktion
