# TrueFX Tickdaten

## Datenbeschreibung

> Top-of-book, tick-by-tick market data, with fractional pip spreads in millisecond detail
>@ https://www.truefx.com/truefx-historical-downloads/


## API

Abruf von monatlich aufbereiteten .zip-Dateien von der Webseite.

## Dateistruktur
- Währungspaar
- Datum/Uhrzeit ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit)) mit einer Genauigkeit von 1 ms
- Geld- und Briefkurs
- Beispiel EUR/USD:

---
    EUR/USD,20130101 21:59:59.981,1.32023,1.32054
    EUR/USD,20130101 21:59:59.996,1.32027,1.32051
    EUR/USD,20130101 22:00:00.296,1.32027,1.32051
	[...]
---
