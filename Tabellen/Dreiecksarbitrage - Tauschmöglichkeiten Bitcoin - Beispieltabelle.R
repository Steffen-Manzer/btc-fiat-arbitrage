library("fst")
library("data.table")
library("zoo") # rollapply
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/FindLastDatasetBeforeTimestamp.R")
source("Funktionen/printf.R")

# Betrachtetes Zeitfenster
# Unterscheidet sich von der Raumarbitrage, da bei dem dortigen Zeitfenster
# kein Devisenhandel stattfand
timeframe      <- c("2021-12-05 22:30:03.320500", "2021-12-05 22:30:04.259")
timeframeForex <- c("2021-12-05 22:30:03",        "2021-12-05 22:30:04.3")

# Schwellwert
bitcoinComparisonThresholdSeconds <- 5L

# Beispieldaten laden
# Hier: Coinbase Pro, da dort anschaulich auch mehrere
# Ticks zum selben Zeitpunkt auftreten
a <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = TRUE
)
pair_btc_a <- "btcusd"

b <- read_fst(
    "Cache/coinbase/btceur/tick/coinbase-btceur-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = TRUE
)
pair_btc_b <- "btceur"

forex <- read_fst(
    "Cache/forex-combined/eurusd/tick/forex-combined-eurusd-tick-2021-12.fst",
    columns = c("Time", "Bid", "Ask"),
    as.data.table = TRUE
)
pair_a_b <- "eurusd"

# Auf Beispiel-Zeitfenster beschränken
a <- a[Time %between% timeframe]
b <- b[Time %between% timeframe]
forex <- forex[Time %between% timeframeForex]

# Gleiche Zeitpunkte gruppieren und Kurspaar vermerken
a <- a[j=.(PriceLow=min(Price), PriceHigh=max(Price), n=.N), by=Time]
a[, CurrencyPair:="btcusd"]
b <- b[j=.(PriceLow=min(Price), PriceHigh=max(Price), n=.N), by=Time]
b[, CurrencyPair:="btceur"]

# Schönen Zeitpunkt für Darstellung finden
# a_ <- a[j=.(Time)]
# a_$Type <- "BTC/USD"
# b_ <- b[j=.(Time)]
# b_$Type <- "BTC/EUR"
# c_ <- forex[j=.(Time)]
# c_$Type = "EUR/USD"
# abc <- rbindlist(list(b_, c_))
# setorder(abc, Time)
# View(abc)
# rm(a_, b_, c_, abc)
# stop()

# Beide Tabellen zusammenführen, hier nach Kurspaar.
# Dokumentation: Siehe `Funktionen/MergeSortAndFilter.R`
ab <- rbindlist(list(a, b))
setorder(ab, Time)
triplets <- rollapply(
    ab$CurrencyPair,
    width = 3,
    # Es handelt sich um ein zu entfernendes Tripel, wenn das Kurspaar
    # im vorherigen, aktuellen und nächsten Tick identisch ist
    FUN = function(x) all(x == x[1]),
    fill = FALSE
)
ab <- ab[!triplets]

# Unterschiede berechnen.
# Dokumentation: Siehe `Raumarbitrage/Bitcoin - Preisunterschiede berechnen.r`
result <- data.table()
numRows <- nrow(ab)
comparisonThreshold <- 5
currentRow <- 0L
while (TRUE) {
    
    # Zähler erhöhen
    currentRow <- currentRow + 1L
    nextRow <- currentRow + 1L
    
    # Ende des Datensatzes erreicht
    if (currentRow == numRows) {
        break
    }
    
    # Aktuelle und nächste Zeile vergleichen
    tick_btc_1 <- ab[currentRow]
    tick_btc_2 <- ab[nextRow]
    
    # Gleiches Kurspaar, überspringe.
    if (tick_btc_1$CurrencyPair == tick_btc_2$CurrencyPair) {
        next
    }
    
    # Zeitdifferenz zwischen den Bitcoin-Ticks zu groß, überspringe.
    bitcoinTimeDifference <- difftime(tick_btc_2$Time, tick_btc_1$Time, units="secs")
    if (bitcoinTimeDifference > bitcoinComparisonThresholdSeconds) {
        numDatasetsOutOfBitcoinThreshold <- numDatasetsOutOfBitcoinThreshold + 1L
        next
    }
    
    # Letzten gültigen Wechselkurs heraussuchen
    tick_ab <- findLastDatasetBeforeTimestamp(forex, tick_btc_2$Time)
    
    # Set in Ergebnisvektor speichern
    if (tick_btc_1$CurrencyPair == pair_btc_a) {
        
        # Tick 1 ist BTC/A, Tick 2 ist BTC/B
        # tick_btc_a <- tick_btc_1
        # tick_btc_b <- tick_btc_2
        result <- rbind(result, list(
            Time = tick_btc_2$Time,
            firstTick = "a",
            a_PriceLow = tick_btc_1$PriceLow,
            a_PriceHigh = tick_btc_1$PriceHigh,
            b_PriceLow = tick_btc_2$PriceLow,
            b_PriceHigh = tick_btc_2$PriceHigh,
            ab_Bid = tick_ab$Bid,
            ab_Ask = tick_ab$Ask
        ))
        
    } else {
        
        # Tick 1 ist BTC/B, Tick 2 ist BTC/A
        # tick_btc_a <- tick_btc_2
        # tick_btc_b <- tick_btc_1
        result <- rbind(result, list(
            Time = tick_btc_2$Time,
            firstTick = "b",
            a_PriceLow = tick_btc_2$PriceLow,
            a_PriceHigh = tick_btc_2$PriceHigh,
            b_PriceLow = tick_btc_1$PriceLow,
            b_PriceHigh = tick_btc_1$PriceHigh,
            ab_Bid = tick_ab$Bid,
            ab_Ask = tick_ab$Ask
        ))
        
    }
}
View(result)
stop()

# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(result))) {
    priceDifference <- result[i]
    printf(
        "%s%s &\n",
        tabIndentFirst,
        formatPOSIXctWithFractionalSeconds(priceDifference$Time, "%H:%M:%OS")
    )
    
    # Höchstkurs
    printf("%s%s &\n", tabIndent, priceDifference$ExchangeHigh)
    printf("%s%s\\,USD &\n", tabIndent, format.money(priceDifference$PriceHigh, digits=2))
    
    # Tiefstkurs
    printf("%s%s &\n", tabIndent, priceDifference$ExchangeLow)
    printf("%s%s\\,USD &\n", tabIndent, format.money(priceDifference$PriceLow, digits=2))
    
    printf("\n")
}
