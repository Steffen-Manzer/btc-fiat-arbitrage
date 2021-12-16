# Dukascopy Tickdaten

## Datenbeschreibung

> The available data includes bid and ask prices and trading volumes for a variety of Forex instruments, commodity, stock, and index CFDs.
> Additionally, the candlestick options allow to fine-tune the data frequency with a wide range of periods from one tick to 11 months.
>@ https://www.dukascopy.com/trading-tools/widgets/quotes/historical_data_feed


## API

Nutzung des Node.js-Moduls [_dukascopy-node_](https://www.npmjs.com/package/dukascopy-node).

## Enthaltener Zeitraum

Alle Datensätze enthalten Daten von 01.01.2010, 00:00:00.000 (UTC) bis heute.

## Dateistruktur
- Datum/Uhrzeit als [Unix-Timestamp](https://de.wikipedia.org/wiki/Unixzeit) mit Millisekunden (10<sup>-3</sup>, letzte drei Stellen)
- Geld- und Briefkurs
- Angebots- und Nachfragemenge in Millionen Einheiten der Basiswährung
- Beispiel USD/CHF: 

---
	timestamp,askPrice,bidPrice,askVolume,bidVolume
	1598918401100,0.90412,0.90403,1.69,1
	1598918401202,0.90412,0.90403,3,1
	[...]
---
