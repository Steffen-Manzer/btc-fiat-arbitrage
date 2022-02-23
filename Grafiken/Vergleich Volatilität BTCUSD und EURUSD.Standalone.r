library("TTR") # Bibliothek "Technical Trading Rules" laden
library("fst") # Bibliothek "fst" für zwischengespeicherte Daten

# Bitcoin-Tagesdaten (BTC/USD) laden
# Datenstruktur:
#          Time       Mean   Open  High   Low  Close  NumDatasets
# 1: 2010-07-18  0.06456250  0.05  0.09  0.05   0.09         1440
# 2: 2010-07-19  0.08863889  0.09  0.09  0.08   0.08         1440
# 3: 2010-07-20  0.07839583  0.08  0.08  0.07   0.07         1440
# 4: ...
btcusd <- read_fst(
    "Cache/coindesk/bpi-daily-btcusd.fst",
    as.data.table = TRUE
)

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


# Führe die gleiche Berechnung analog für EUR/USD durch.
# Datenstruktur:
#          Time  CloseBid  CloseAsk  CloseMittel  numDatasets
# 1: 2010-01-01   1.43335   1.43345      1.43340        27354
# 2: 2010-01-03   1.43141   1.43149      1.43145         1078
# 3: 2010-01-04   1.44244   1.44254      1.44249        28432
# 4: ...
eurusd <- read_fst(
    "Cache/Dukascopy/eurusd/dukascopy-eurusd-daily.fst",
    as.data.table = TRUE
)

# Berechnung der annualisierten Close-to-close-Volatilität.
# Hier erfolgt der Handel an sechs von sieben Tagen in der Woche,
# der Datensatz enthält pro Jahr somit Werte für 312 Tage.
handelstageProJahr <- 312
eurusd$vClose <- volatility(
    eurusd$CloseMittel,
    n = handelstageProJahr,
    N = handelstageProJahr
)
eurusd <- eurusd[handelstageProJahr:nrow(eurusd)]
