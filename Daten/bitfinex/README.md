# Bitfinex Tickdaten

## Datenbeschreibung

> The trades endpoint allows the retrieval of past public trades and includes
> details such as price, size, and time.
>@ https://docs.bitfinex.com/reference#rest-public-trades

Die Dateien ohne Monatsangabe enthalten alle Daten bis zur Umstellung auf monatsbasierte CSV-Dateien.

## API

- Abfrage folgender URLs:
    - https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist
    - https://api-pub.bitfinex.com/v2/trades/tBTCEUR/hist
    - https://api-pub.bitfinex.com/v2/trades/tBTCGBP/hist
    - https://api-pub.bitfinex.com/v2/trades/tBTCJPY/hist
- Aktualisierung erfolgt stündlich bzw. halbstündlich (BTCUSD)

## Enthaltener Zeitraum

- BTCUSD, BTCEUR, BTCGBP und BTCJPY enthalten jeweils Daten von 01.01.2018, 00:00:00 (UTC) bis heute

## Dateistruktur
- Vorgangs-ID
- Datum ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit))
- Gehandelte Menge in BTC ("How much was bought (positive) or sold (negative).")
- Preis in USD/EUR/GBP/JPY

---
    ID,Time,Amount,Price
    148668312,2018-01-01T00:00:00.000000Z,0.01475502,13769
    148668314,2018-01-01T00:00:01.000000Z,-0.1,13763
---
