# Coindesk Bitcoin Price Index

## Datenbeschreibung

> Launched in September 2013, the CoinDesk Bitcoin Price Index (XBP) represents an average
> of bitcoin prices across leading global exchanges that meet criteria specified by the XBP.
> It is intended to serve as a standard retail price reference for industry participants and
> accounting professionals.
>@ https://www.coindesk.com/price/bitcoin-price-index

## API
- Abfrage folgender URL: https://api.coindesk.com/charts/data?data=close&exchanges=bpi&index=USD&dev=1
- Aktualisierung erfolgt täglich um 08:02
- Aufgrund der Nutzung einer internen API können bei kurzfristigen Änderungen vorübergehend Lücken entstehen.

## Enthaltener Zeitraum

- BTCUSD enthält Daten von 18.07.2010, 00:00:00 (UTC) bis heute

## Dateistruktur
- Datum ([UTC](https://de.wikipedia.org/wiki/Koordinierte_Weltzeit))
- Aktueller Wert des BPI in USD

---
    Time,Close
    2010-07-18T00:00:00.000000+00:00,0.05
    2010-07-18T00:01:00.000000+00:00,0.05
    [...]
    2019-10-27T23:58:00.000000+00:00,9545.54
    2019-10-27T23:59:00.000000+00:00,9562.93
---
