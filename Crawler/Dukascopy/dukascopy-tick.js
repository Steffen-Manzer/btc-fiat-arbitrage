// https://www.npmjs.com/package/dukascopy-node
const { getHistoricRates } = require('dukascopy-node');
const fs = require('fs');
const { exec, execSync } = require('child_process');

// Einstellungen
const config = require('./config-tick.js');
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
	console.log(`    Jahre ${config.yearFrom} - ${config.yearTo}`);
	console.log(`    Wechselkurspaare: ${config.pairs.join(", ")}`);
	console.log(`    Datenpfad: ${config.targetDirectory}`);
	
	let numDatasetsProcessed = 0;
	let gzipAsyncCalled = false;
	for (let pair of config.pairs) {
		console.log(`\nStarte Wechselkurs: ${pair}`);
		
		// Zielverzeichnis erstellen, falls es noch nicht existiert
		if (!fs.existsSync(`${config.targetDirectory}${pair.toUpperCase()}`)) {
			fs.mkdirSync(`${config.targetDirectory}${pair.toUpperCase()}`);
		}
		
		for (let year = config.yearFrom; year <= config.yearTo; year++) {
			for (let month = 1; month <= 12; month++) {
				
				// Zeitmessung
				let tStart = new Date();
				
				// Formatiertes Datum
				let monthPadded = month.toString().padStart(2, 0);
				
				// Ziel-CSV
				let targetFile = `${config.targetDirectory}${pair.toUpperCase()}/${pair.toUpperCase()}-${year}-${monthPadded}.csv`;
				
				if (fs.existsSync(`${targetFile}.gz`)) {
					//console.log('Daten existieren, überspringe.');
					continue;
				}
				
				// In Konsole ohne neue Zeile am Ende schreiben
				process.stdout.write(`    ${pair.toUpperCase()} ${year}-${monthPadded}... `);
				
				// Zeitrahmen spezifizieren
				let fromTo = {
					from: `${year}-${monthPadded}-01`,
					to: `${year}-${(month+1).toString().padStart(2, 0)}-01`
				};
				
				if (month === 12) {
					fromTo = {
						from: `${year}-${monthPadded}-01`,
						to: `${year+1}-01-01`
					};
				}
				
				// Monat noch nicht abgeschlossen
				if ((new Date(fromTo.to)) > now) {
					console.log(`Datum in der Zukunft, überspringe.`);
					continue;
				}
				
				
				// Dukascopy anfragen
				let data;
				try {
					data = await getHistoricRates({
						instrument: pair,
						dates: fromTo,
						timeframe: 'tick',
						format: 'csv'
					});
				} catch(error) {
					console.error(`\n*** Fehler: ${error}`);
				};
				
				process.stdout.write("Schreibe in Datei... ");
				
				// Rohdaten in .csv schreiben
				fs.writeFileSync(targetFile, data);
				if (fs.statSync(targetFile).size === 0) {
					console.error(`\n*** Keine Daten geschrieben!`);
					continue;
				}
				
				// Wenn möglich, asynchron komprimieren
				process.stdout.write("Komprimiere... ");
				if (sigintCaptured) {
					execSync(`/usr/bin/gzip ${targetFile}`);
				} else {
					gzipAsyncCalled = true;
					exec(`/usr/bin/gzip ${targetFile}`, (error, stdout, stderr) => {
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
				console.log(`Abgeschlossen. ${duration}s vergangen.`);
				
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
				
			} // Monate 
		} // Jahre
	} // Wechselkurse
	
	console.log("Alle Paare abgeschlossen.");
	
	if (gzipAsyncCalled) {
		console.log("Warte 10s, um verbleibende gzip-Operationen abzuschließen.");	
		await new Promise(resolve => setTimeout(resolve, 10000));
	}
	
})(); // Hauptfunktion
