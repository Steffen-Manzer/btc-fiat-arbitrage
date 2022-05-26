library("fst")
library("data.table")
library("zoo") # rollapply
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/printf.R")

# Betrachtetes Zeitfenster
# Unterscheidet sich von der Raumarbitrage, da bei dem dortigen Zeitfenster
# kein Devisenhandel stattfand
timeframe <- c("2021-12-05 23:06:01.086400", "2021-12-05 23:06:02.17022")

# Beispieldaten laden
# Hier: Coinbase Pro, da dort anschaulich auch mehrere
# Ticks zum selben Zeitpunkt auftreten
a <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = TRUE
)
b <- read_fst(
    "Cache/coinbase/btceur/tick/coinbase-btceur-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = TRUE
)

# Auf Beispiel-Zeitfenster beschränken
a <- a[Time %between% timeframe]
b <- b[Time %between% timeframe]

# Gleiche Ticks gruppieren und ursprüngliches Paar vermerken
a <- a[j=.(PriceLow=min(Price), PriceHigh=max(Price), n=.N), by=Time]
a[, CurrencyPair:="BTC/USD"]
b <- b[j=.(PriceLow=min(Price), PriceHigh=max(Price), n=.N), by=Time]
b[, CurrencyPair:="BTC/EUR"]

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

# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(ab))) {
    tick <- ab[i]
    printf(
        "%s%s &\n",
        tabIndentFirst, formatPOSIXctWithFractionalSeconds(tick$Time, "%d.%m.%Y, %H:%M:%OS")
    )
    printf(
        "%s%s\\,%s &\n",
        tabIndent, format.money(tick$PriceHigh, digits=2), substr(tick$CurrencyPair, 5, 7)
    )
    printf(
        "%s%s\\,%s &\n",
        tabIndent, format.money(tick$PriceLow, digits=2), substr(tick$CurrencyPair, 5, 7)
    )
    printf("%s%d &\n", tabIndent, tick$n)
    # Keine Ticks gefiltert, daher hier einfach abbrechen
    #printf("%s%s &\n", tabIndent, tick$CurrencyPair)
    printf("%s%s \\\\\n", tabIndent, tick$CurrencyPair)
    
    # if (isTRUE(triplets[i])) {
    #     printf("%s* \\\\\n", tabIndent)
    # } else {
    #     printf("%s\\\\\n", tabIndent)
    # }
    
    printf("\n")
}
