# Coinbase Pro Tickdaten

## Datenbeschreibung

> List the latest trades for a product.  
> The trade side indicates the maker order side. The maker order is the order that was open on the order book.  
> **buy** side (0) indicates a down-tick because the maker was a buy order and their order was removed.  
> Conversely, **sell** side (1) indicates an up-tick.
>@ https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproducttrades

## API

Abfrage folgender URLs:
- https://api.pro.coinbase.com/products/BTC-USD/trades
- https://api.pro.coinbase.com/products/BTC-EUR/trades
- https://api.pro.coinbase.com/products/BTC-GBP/trades

## Enthaltener Zeitraum

- BTCUSD enthält Daten von 01.12.2014, 05:33:56.761199 (UTC) bis heute.
- BTCEUR enthält Daten von 23.04.2015, 01:42:34.182104 (UTC) bis heute.
- BTCGBP enthält Daten von 21.04.2015, 22:22:41.294060 (UTC) bis heute.

## Dateistruktur
- Vorgangs-ID
- Datum/Uhrzeit ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit)) mit einer Genauigkeit von 1 ms
- Gehandelte Menge in BTC
- Preis in USD/EUR/GBP
- Art (Kauf = 0 / Verkauf = 1)

Anmerkung: Die Daten sind nicht strikt chronologisch sortiert, Abweichungen auf ms-Basis können bestehen.

---
    ID,Time,Amount,Price,Type
    31503403,2017-12-31T23:59:03.515000Z,0.00337300,13922.51000000,0
    31503404,2017-12-31T23:59:03.515000Z,0.00337300,13922.51000000,0
    [...]
---
