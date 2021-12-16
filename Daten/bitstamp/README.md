# Bitstamp Tickdaten

> Returns data for the requested currency pair.  
> [...]  
> Supported values for currency_pair: btcusd, btceur, [...]
>
> Response (JSON) - descending list of transactions. Every transaction dictionary contains:  
> - date: Unix timestamp date and time.
> - tid: Transaction ID.
> - price: BTC price.
> - amount: BTC amount.
> - type: 0 (buy) or 1 (sell).
>
>@ https://www.bitstamp.net/api/#transactions

Die Dateien ohne Monatsangabe enthalten alle Daten bis zur Umstellung auf monatsbasierte CSV-Dateien.

## API

- Abfrage folgender URLs:
    - https://www.bitstamp.net/api/v2/transactions/btcusd/?time=day
    - https://www.bitstamp.net/api/v2/transactions/btceur/?time=day
- Aktualisierung erfolgt alle 7 Stunden

## Enthaltener Zeitraum

- BTCUSD enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute
- BTCEUR enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute

## Dateistruktur
- Vorgangs-ID
- Datum ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit))
- Gehandelte Menge in BTC
- Preis in USD bzw. EUR
- Art (Kauf = 0 / Verkauf = 1)

---
    ID,Time,Amount,Price,Type
    96551989,2019-09-01T00:00:07+00:00,0.00927907,8755.71,1
    96551990,2019-09-01T00:00:09+00:00,0.01970993,8750.82,1
    [...]
    101212130,2019-11-29T14:25:24+00:00,0.09000000,7754.09,0
    101212131,2019-11-29T14:25:24+00:00,0.00646000,7754.09,0
---
