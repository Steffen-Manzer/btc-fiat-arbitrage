# Bitstamp-Tickdaten via Bitcoincharts

> Bitcoincharts provides a simple API to most of its data.  
> [...]  
> **Historic Trade Data**  
> Trade data is available as CSV, delayed by approx. 15 minutes. It will return the 2000 most recent trades.  
>@ https://bitcoincharts.com/about/markets-api/

Diese Schnittstelle wird für länger zurückliegende Tickdaten genutzt. Sie bietet im Unterschied zur offiziellen API
keine Unterscheidung von Kauf- und Verkaufsgeschäften.


## API
- Nutzung folgender Adressen:
    - http://api.bitcoincharts.com/v1/trades.csv?symbol=bitstampUSD
    - http://api.bitcoincharts.com/v1/trades.csv?symbol=bitstampEUR
- Aktualisierung erfolgt einmal pro Stunde

## Enthaltener Zeitraum

- BTCUSD enthält Daten von 13.09.2011, 13:53:36 (UTC) bis heute
- BTCEUR enthält Daten von 05.12.2017, 11:43:49 (UTC) bis heute
- BTCEUR weist eine **Lücke** zwischen 01.02.2020 und 14.03.2020 auf

## Dateistruktur

- Datum/Uhrzeit ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit)) mit einer Genauigkeit von 1 s
- Preis in USD/EUR
- Menge in BTC
- Beispiel:

---
    Time,Price,Amount
    2017-12-05T11:43:49.000000+00:00,9803.92,0.137166
    2017-12-05T11:44:01.000000+00:00,9842.66,0.13538904
    [...]
---
