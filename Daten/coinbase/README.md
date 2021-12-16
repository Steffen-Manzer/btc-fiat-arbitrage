# Coinbase Pro Tickdaten

## Datenbeschreibung

> List the latest trades for a product.  
> The trade side indicates the maker order side. The maker order is the order that was open on the order book.  
> **buy** side (0) indicates a down-tick because the maker was a buy order and their order was removed.  
> Conversely, **sell** side (1) indicates an up-tick.
>@ https://docs.pro.coinbase.com/#get-trades

## API

- Abfrage folgender URLs:
    - https://api.pro.coinbase.com/products/BTC-USD/trades
    - https://api.pro.coinbase.com/products/BTC-EUR/trades
    - https://api.pro.coinbase.com/products/BTC-GBP/trades
- Aktualisierung erfolgt 1-3 Minuten (je nach Handelspaar)

## Enthaltener Zeitraum

- BTCUSD enthält Daten von 31.12.2017, 23:59:55.819 (UTC) bis heute
- BTCEUR enthält Daten von 31.12.2017, 23:59:50.700 (UTC) bis heute
- BTCGBP enthält Daten von 31.12.2017, 23:59:50.373 (UTC) bis heute

## Dateistruktur
- Vorgangs-ID
- Datum ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit))
- Gehandelte Menge in BTC
- Preis in USD/EUR/GBP
- Art (Kauf = 0 / Verkauf = 1)

---
    ID,Time,Amount,Price,Type
    31503403,2017-12-31T23:59:03.515000Z,0.00337300,13922.51000000,0
    31503404,2017-12-31T23:59:03.515000Z,0.00337300,13922.51000000,0
    ...
---
