# Dukascopy Tickdaten

## Datenbeschreibung

> The available data includes bid and ask prices and trading volumes for a variety of Forex instruments, commodity, stock, and index CFDs.
> Additionally, the candlestick options allow to fine-tune the data frequency with a wide range of periods from one tick to 11 months.
>@ https://www.dukascopy.com/trading-tools/widgets/quotes/historical_data_feed


## API

Abfrage folgender URL:

> `https://datafeed.dukascopy.com/datafeed/{SYMBOL}/{JAHR}/{MONAT}/{TAG}/{STUNDE}h_ticks.bi5`

dabei ist

- `{SYMBOL}` die ISO~4217-Codes des gewünschten Handelspaares, etwa EURUSD,
- `{JAHR}` das gewünschte Jahr als vierstellige Zahl,
- `{MONAT}` der gewünschte Monat als zweistellige Zahl, wobei 0 für Januar und 11 für Dezember steht und
- `{TAG}` bzw. `{STUNDE}` der gewünschte Tag respektive die gewünschte Stunde als zweistellige Zahl.

## Dateistruktur
- Datum/Uhrzeit ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit)) mit einer Genauigkeit von 1 ms
- Geld- und Briefkurs
- Angebots- und Nachfragemenge in Einheiten der Basiswährung
- Beispiel EUR/USD: 

---
    Time,Bid,Ask,BidVolume,AskVolume
    2013-01-01T00:00:00.463000Z,1.3195,1.31962,1500000,1500000
    2013-01-01T00:00:07.974000Z,1.31951,1.31963,1500000,2250000
    2013-01-01T00:00:08.095000Z,1.31945,1.31959,1500000,1500000
    [...]
---
