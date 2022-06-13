library("fst")
library("data.table")
library("zoo") # rollapply
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/printf.R")

# Betrachtetes Zeitfenster
timeframe <- c("2021-12-05 19:35:12.097997", "2021-12-05 19:35:14.505134")

# Beispieldaten laden
a <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-12.fst",
    columns = c("ID", "Time", "Price"), 
    as.data.table = T
)
a[, Exchange:="Coinbase Pro"]

b <- read_fst(
    "Cache/bitfinex/btcusd/tick/bitfinex-btcusd-tick-2021-12.fst",
    columns = c("ID", "Time", "Price"), 
    as.data.table = T
)
b[, Exchange:="Bitfinex"]

# Auf Beispiel-Zeitfenster beschränken
a <- a[Time %between% timeframe]
b <- b[Time %between% timeframe]

# Gleiche Zeitpunkte gruppieren
a <- a[
    j=.(
        PriceHigh = max(Price),
        PriceLow = min(Price),
        Exchange = last(Exchange)
    ), 
    by=Time
]
b <- b[
    j=.(
        PriceHigh = max(Price),
        PriceLow = min(Price),
        Exchange = last(Exchange)
    ), 
    by=Time
]

# Beide Tabellen zusammenführen.
# Dokumentation: Siehe `Raumarbitrage/Bitcoin - Preisunterschiede berechnen.r`
ab <- rbindlist(list(a, b), use.names=TRUE)
setorder(ab, Time)
triplets <- c(
    FALSE,
    rollapply(
        ab$Exchange,
        width = 3,
        # Es handelt sich um ein zu entfernendes Tripel, wenn  die
        # Börse im vorherigen, aktuellen und nächsten Tick identisch ist
        FUN = function(exchg) (exchg[1] == exchg[2] && exchg[2] == exchg[3])
    ),
    FALSE
)
ab <- ab[!triplets]

# Unterschiede berechnen.
# Dokumentation: Siehe `Raumarbitrage/Bitcoin - Preisunterschiede berechnen.r`
result <- data.table()
numRows <- nrow(ab)
comparisonThreshold <- 2L
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
    tick_a <- ab[currentRow]
    tick_b <- ab[nextRow]
    
    # Gleiche Börse, überspringe.
    if (tick_a$Exchange == tick_b$Exchange) {
        next
    }
    
    # Zeitdifferenz zu groß, überspringe.
    if (difftime(tick_b$Time, tick_a$Time, units="secs") > comparisonThreshold) {
        next
    }
    
    if (
        tick_a$PriceHigh - tick_b$PriceLow >=
        tick_b$PriceHigh - tick_a$PriceLow
    ) {
        result <- rbind(result, list(
            Time = tick_b$Time,
            PriceHigh = tick_a$PriceHigh,
            ExchangeHigh = tick_a$Exchange,
            PriceLow = tick_b$PriceLow,
            ExchangeLow = tick_b$Exchange
        ))
    } else {
        result <- rbind(result, list(
            Time = tick_b$Time,
            PriceHigh = tick_b$PriceHigh,
            ExchangeHigh = tick_b$Exchange,
            PriceLow = tick_a$PriceLow,
            ExchangeLow = tick_a$Exchange
        ))
    }
}


# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(result))) {
    priceDifference <- result[i]
    printf(
        "%s%s &\n",
        tabIndentFirst, formatPOSIXctWithFractionalSeconds(priceDifference$Time, "%H:%M:%OS")
    )
    
    # Höchstkurs
    printf("%s%s &\n", tabIndent, priceDifference$ExchangeHigh)
    printf("%s%s\\,USD &\n", tabIndent, format.money(priceDifference$PriceHigh, digits=2))
    
    # Tiefstkurs
    printf("%s%s &\n", tabIndent, priceDifference$ExchangeLow)
    printf("%s%s\\,USD &\n", tabIndent, format.money(priceDifference$PriceLow, digits=2))
    
    # Absolute Differenz
    printf("%s%s\\,USD &\n", tabIndent, 
           format.money(priceDifference$PriceHigh - priceDifference$PriceLow, digits=2))
    
    # Relative Abweichung
    printf("%s%s\\,\\%% \\\\\n", tabIndent, 
           format.percentage(priceDifference$PriceHigh / priceDifference$PriceLow - 1, 3L))
    
    printf("\n")
}
