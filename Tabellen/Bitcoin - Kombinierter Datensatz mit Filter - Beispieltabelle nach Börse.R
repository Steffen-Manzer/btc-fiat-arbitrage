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
    columns = c("Time", "Price"), 
    as.data.table = TRUE
)
b <- read_fst(
    "Cache/bitfinex/btcusd/tick/bitfinex-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = TRUE
)

# Auf Beispiel-Zeitfenster beschränken
a <- a[Time %between% timeframe]
b <- b[Time %between% timeframe]

# Gleiche Ticks gruppieren und ursprüngliche Börse vermerken
a <- a[j=.(PriceLow=min(Price),PriceHigh=max(Price),n=.N),by=Time]
a[, Exchange:="Coinbase Pro"]
b <- b[j=.(PriceLow=min(Price),PriceHigh=max(Price),n=.N),by=Time]
b[, Exchange:="Bitfinex"]

# Beide Tabellen zusammenführen, hier nach Börse.
# Dokumentation: Siehe `Funktionen/MergeSortAndFilter.R`
ab <- rbindlist(list(a, b))
setorder(ab, Time)
triplets <- rollapply(
    ab$Exchange,
    width = 3,
    # Es handelt sich um ein zu entfernendes Tripel, wenn  die
    # Börse im vorherigen, aktuellen und nächsten Tick identisch ist
    FUN = function(x) all(x == x[1]),
    fill = FALSE
)


# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(ab))) {
    tick <- ab[i]
    printf("%s%s &\n",
           tabIndentFirst, 
           formatPOSIXctWithFractionalSeconds(tick$Time, "%d.%m.%Y, %H:%M:%OS")
    )
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$PriceHigh, digits=2))
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$PriceLow, digits=2))
    # Anzahl Ticks ist für die Raumarbitrage nicht von Interesse
    #printf("%s%d &\n", tabIndent, tick$n)
    printf("%s%s &\n", tabIndent, tick$Exchange)
    
    if (isTRUE(triplets[i])) {
        printf("%s* \\\\\n", tabIndent)
    } else {
        printf("%s\\\\\n", tabIndent)
    }
    
    printf("\n")
}
