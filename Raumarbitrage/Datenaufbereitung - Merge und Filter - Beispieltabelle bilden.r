library("fst")
library("data.table")
library("zoo")
source("Funktionen/NumberFormat.r")
source("Funktionen/printf.r")

# Beispieldaten laden
a <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = T
)
b <- read_fst(
    "Cache/bitfinex/btcusd/tick/bitfinex-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = T
)

# Auf Beispiel-Zeitfenster beschränken
a <- a[Time>="2021-12-05 19:35:12.097997" & Time<"2021-12-05 19:35:14.505135",]
b <- b[Time>="2021-12-05 19:35:12.097997" & Time<"2021-12-05 19:35:14.505135",]

# Gleiche Ticks gruppieren und ursprüngliche Börse vermerken
a <- a[j=.(PriceLow=min(Price),PriceHigh=max(Price),n=.N),by=Time]
a[, Exchange:="Coinbase Pro"]
b <- b[j=.(PriceLow=min(Price),PriceHigh=max(Price),n=.N),by=Time]
b[, Exchange:="Bitfinex"]

# Beide Tabellen zusammenführen.
# Dokumentation: Siehe `Preisunterschiede Bitcoin-Börsen berechnen.r`
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


# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- "        " # rep() funktioniert mit sprintf nicht korrekt
tabIndent <- "            " 
for (i in seq_len(nrow(ab))) {
    tick <- ab[i,]
    printf("%s%s &\n", tabIndentFirst, format(tick$Time, "%d.%m.%Y, %H:%M:%OS"))
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$PriceLow, digits=2))
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$PriceHigh, digits=2))
    printf("%s%d &\n", tabIndent, tick$n)
    printf("%s%s &\n", tabIndent, tick$Exchange)
    
    if (isTRUE(triplets[i])) {
        printf("%sNicht relevant \\\\\n", tabIndent)
    } else {
        printf("%s\\\\\n", tabIndent)
    }
    
    printf("\n")
}
