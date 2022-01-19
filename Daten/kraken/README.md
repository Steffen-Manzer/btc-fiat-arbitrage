# Kraken Tickdaten

## Datenbeschreibung

> Get recent trades  
> Result: array of pair name and recent trade data
>@ https://www.kraken.com/features/api#get-recent-trades

Die Dateien ohne Monatsangabe enthalten alle Daten bis zur Umstellung auf monatsbasierte CSV-Dateien.

## API

- Abfrage folgender URLs:
    - https://api.kraken.com/0/public/Trades?pair=xbtusd
    - https://api.kraken.com/0/public/Trades?pair=xbteur
    - https://api.kraken.com/0/public/Trades?pair=xbtgbp
    - https://api.kraken.com/0/public/Trades?pair=xbtjpy
    - https://api.kraken.com/0/public/Trades?pair=xbtcad
    - https://api.kraken.com/0/public/Trades?pair=xbtchf
- EUR wird alle 20 Minuten aktualisiert
- Alle anderen Handelspaare werden alle 30 Minuten aktualisiert

## Enthaltener Zeitraum

- BTCUSD enthält Daten von 06.10.2013, 21:34:15 (UTC) bis heute
- BTCEUR enthält Daten von 10.09.2013, 23:47:11 (UTC) bis heute
- BTCGBP enthält Daten von 06.11.2014, 16:13:43 (UTC) bis heute
- BTCJPY enthält Daten von 05.11.2014, 22:21:30 (UTC) bis heute
- BTCCAD enthält Daten von 29.06.2015, 03:27:41 (UTC) bis heute
- BTCCHF enthält Daten von 06.12.2019, 16:33:17 (UTC) bis heute
- BTCAUD enthält Daten von 16.06.2020, 22:30:13 (UTC) bis heute

## Dateistruktur
- Datum/Uhrzeit ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit)) mit einer Genauigkeit von 100 µs
- Gehandelte Menge in BTC
- Preis in USD/EUR/GBP/JPY/CAD/CHF/AUD
- Art (Kauf = b / Verkauf = s)
- Limit (l = Limitiert, m = Market = Unlimitiert)

---
    Time,Amount,Price,Type,Limit
    2013-10-06T21:34:15.551400Z,0.10000000,122.00000,s,l
    2013-10-07T20:50:30.481500Z,0.10000000,123.61000,s,l
    [...]
---
