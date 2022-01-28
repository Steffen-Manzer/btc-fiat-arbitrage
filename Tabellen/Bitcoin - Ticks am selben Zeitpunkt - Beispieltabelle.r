library("fst")
library("data.table")
source("Funktionen/FormatNumber.r")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.r")
source("Funktionen/printf.r")

# Beispieldaten laden
d <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-12.fst",
    columns = c("Time", "Price"), 
    as.data.table = T
)

# Auf Beispiel-Zeitfenster beschränken
d <- d[Time %between% c("2021-12-05 19:35:02.226903", "2021-12-05 19:35:02.936298")]

# Gruppieren
grouped <- d[j=.(PriceLow=min(Price),PriceHigh=max(Price),n=.N),by=Time]

# Beide Tabellen zusammenführen
joined <- d[grouped,on="Time"]
#                          Time    Price PriceLow PriceHigh n
# 1: 2021-12-05 19:35:02.226903 48561.35 48561.35  48561.35 1
# 2: 2021-12-05 19:35:02.557620 48570.61 48570.61  48570.61 1
# 3: 2021-12-05 19:35:02.619364 48570.61 48570.61  48572.34 3
# 4: 2021-12-05 19:35:02.619364 48570.63 48570.61  48572.34 3
# 5: 2021-12-05 19:35:02.619364 48572.34 48570.61  48572.34 3
# 6: 2021-12-05 19:35:02.632916 48570.62 48570.62  48570.62 1
# 7: 2021-12-05 19:35:02.885982 48572.34 48572.34  48573.18 2
# 8: 2021-12-05 19:35:02.885982 48573.18 48572.34  48573.18 2
# 9: 2021-12-05 19:35:02.936297 48571.10 48571.10  48571.10 1

# Behalte nur jeweils letzte Spalte nach Join bei. Händisch:
joined[3:4,`:=`(PriceLow=NA,PriceHigh=NA,n=NA)]
joined[7,`:=`(PriceLow=NA,PriceHigh=NA,n=NA)]
#                          Time    Price PriceLow PriceHigh  n
# 1: 2021-12-05 19:35:02.226903 48561.35 48561.35  48561.35  1
# 2: 2021-12-05 19:35:02.557620 48570.61 48570.61  48570.61  1
# 3: 2021-12-05 19:35:02.619364 48570.61       NA        NA NA
# 4: 2021-12-05 19:35:02.619364 48570.63       NA        NA NA
# 5: 2021-12-05 19:35:02.619364 48572.34 48570.61  48572.34  3
# 6: 2021-12-05 19:35:02.632916 48570.62 48570.62  48570.62  1
# 7: 2021-12-05 19:35:02.885982 48572.34       NA        NA NA
# 8: 2021-12-05 19:35:02.885982 48573.18 48572.34  48573.18  2
# 9: 2021-12-05 19:35:02.936297 48571.10 48571.10  48571.10  1

# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(joined))) {
    tick <- joined[i,]
    printf("%s%s &\n",
           tabIndentFirst,
           formatPOSIXctWithFractionalSeconds(tick$Time, "%d.%m.%Y, %H:%M:%OS")
    )
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$Price, digits=2))
    
    if (is.na(tick$PriceLow)) {
        printf("%s& & \\\\\n", tabIndent)
    } else {
        printf("%s%s\\,USD &\n", tabIndent, format.money(tick$PriceLow, digits=2))
        printf("%s%s\\,USD &\n", tabIndent, format.money(tick$PriceHigh, digits=2))
        printf("%s%s \\\\\n", tabIndent, format.number(tick$n))
    }
    
    printf("\n")
}
