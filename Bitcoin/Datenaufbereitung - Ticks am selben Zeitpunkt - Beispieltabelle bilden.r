library("fst")
library("data.table")
source("Funktionen/NumberFormat.r")
source("Funktionen/printf.r")

# Beispieldaten laden
d <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = T
)

# Auf Beispiel-Zeitfenster beschränken
f <- d[Time>="2021-12-05 19:35:02.632916" & Time<"2021-12-05 19:35:04.196187",]

# Gruppieren
g <- f[j=.(PriceLow=min(Price),PriceHigh=max(Price),n=.N),by=Time]

# Beide Tabellen zusammenführen
j <- f[g,on="Time"]
#                           Time    Price PriceLow PriceHigh
#  1: 2021-12-05 19:35:02.632916 48570.62 48570.62  48570.62
#  2: 2021-12-05 19:35:02.885982 48572.34 48572.34  48573.18
#  3: 2021-12-05 19:35:02.885982 48573.18 48572.34  48573.18
#  4: 2021-12-05 19:35:02.936297 48571.10 48571.10  48571.10
#  5: 2021-12-05 19:35:03.275568 48573.18 48573.18  48579.77
#  6: 2021-12-05 19:35:03.275568 48573.30 48573.18  48579.77
#  7: 2021-12-05 19:35:03.275568 48574.89 48573.18  48579.77
#  8: 2021-12-05 19:35:03.275568 48576.45 48573.18  48579.77
#  9: 2021-12-05 19:35:03.275568 48577.58 48573.18  48579.77
# 10: 2021-12-05 19:35:03.275568 48578.08 48573.18  48579.77
# 11: 2021-12-05 19:35:03.275568 48578.51 48573.18  48579.77
# 12: 2021-12-05 19:35:03.275568 48579.33 48573.18  48579.77
# 13: 2021-12-05 19:35:03.275568 48579.76 48573.18  48579.77
# 14: 2021-12-05 19:35:03.275568 48579.76 48573.18  48579.77
# 15: 2021-12-05 19:35:03.275568 48579.77 48573.18  48579.77
# 16: 2021-12-05 19:35:04.142363 48571.67 48571.67  48571.67

# Behalte nur jeweils letzte Spalte nach Join bei. Händisch:
j[2,`:=`(PriceLow=NA,PriceHigh=NA,n=NA)]
j[5:14,`:=`(PriceLow=NA,PriceHigh=NA,n=NA)]
#                           Time    Price PriceLow PriceHigh
#  1: 2021-12-05 19:35:02.632916 48570.62 48570.62  48570.62
#  2: 2021-12-05 19:35:02.885982 48572.34       NA        NA
#  3: 2021-12-05 19:35:02.885982 48573.18 48572.34  48573.18
#  4: 2021-12-05 19:35:02.936297 48571.10 48571.10  48571.10
#  5: 2021-12-05 19:35:03.275568 48573.18       NA        NA
#  6: 2021-12-05 19:35:03.275568 48573.30       NA        NA
#  7: 2021-12-05 19:35:03.275568 48574.89       NA        NA
#  8: 2021-12-05 19:35:03.275568 48576.45       NA        NA
#  9: 2021-12-05 19:35:03.275568 48577.58       NA        NA
# 10: 2021-12-05 19:35:03.275568 48578.08       NA        NA
# 11: 2021-12-05 19:35:03.275568 48578.51       NA        NA
# 12: 2021-12-05 19:35:03.275568 48579.33       NA        NA
# 13: 2021-12-05 19:35:03.275568 48579.76       NA        NA
# 14: 2021-12-05 19:35:03.275568 48579.76       NA        NA
# 15: 2021-12-05 19:35:03.275568 48579.77 48573.18  48579.77
# 16: 2021-12-05 19:35:04.142363 48571.67 48571.67  48571.67

# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- "        " # rep() funktioniert mit sprintf nicht korrekt
tabIndent <- "            " 
for (i in seq_len(nrow(j))) {
    tick <- j[i,]
    printf("%s%s &\n", tabIndentFirst, format(tick$Time))
    printf("%s%s\\,USD &\n", tabIndent, moneyFormat(tick$Price, digits=2))
    
    if (is.na(tick$PriceLow)) {
        printf("%s& & \\\\\n", tabIndent)
    } else {
        printf("%s%s\\,USD &\n", tabIndent, moneyFormat(tick$PriceLow, digits=2))
        printf("%s%s\\,USD &\n", tabIndent, moneyFormat(tick$PriceHigh, digits=2))
        printf("%s%s \\\\\n", tabIndent, numberFormat(tick$n))
    }
    
    printf("\n")
}
