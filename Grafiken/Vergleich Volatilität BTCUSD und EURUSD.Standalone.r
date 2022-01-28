library("TTR") # Bibliothek "Technical Trading Rules" laden
library("fst") # Bibliothek "fst" für zwischengespeicherte Daten

# Bitcoin-Tagesdaten (BTC/USD) laden. Struktur:
# Time          Mean  Open  High   Low  Close  NumDatasets
# 2010-07-19  0.0886  0.09  0.09  0.08   0.08         1440
# 2010-07-20  [...]
btcusd <- read_fst("Cache/coindesk/bpi-daily-btcusd.fst")

# Berechnung der annualisierten Close-to-close-Volatilität.
# Hier erfolgt der Handel rund um die Uhr, 365 Tage im Jahr.
handelstageProJahr <- 365
btcusd$vClose <- volatility(
    btcusd$Close, 
    n = handelstageProJahr, # "Number of periods for 
                            #  the volatility estimate."
    N = handelstageProJahr  # "Number of periods per year."
)

# Für die ersten 365 Datenpunkte kann keine 1-Jahres-Volatilität
# berechnet werden. Begrenze den Datensatz daher auf gültige Werte.
btcusd <- btcusd[handelstageProJahr:nrow(btcusd)]

# Führe die gleiche Berechnung analog für EUR/USD durch. Struktur:
#       Time     Mean    Open     High      Low   Close  NumDatasets
# 2010-01-04 1.436744 1.43147 1.445580 1.425650 1.44249        28432
# 2010-01-05 [...]
#
# Hier erfolgt der Handel an sechs von sieben Tagen in der Woche,
# der Datensatz enthält pro Jahr somit Werte für 312 Tage.
handelstageProJahr <- 312
eurusd <- read_fst("Cache/Dukascopy/dukascopy-daily-eurusd.fst")
eurusd$vClose <- volatility(
    eurusd$Close, n = handelstageProJahr, N = handelstageProJahr
)
eurusd <- eurusd[handelstageProJahr:nrow(eurusd)]
