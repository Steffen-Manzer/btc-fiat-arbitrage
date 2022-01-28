library("fst")
library("data.table")
source("Funktionen/FormatNumber.r")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.r")
source("Funktionen/printf.r")

# Betrachtungszeitraum
sampleTimeframe <- c(
    as.POSIXct("2020-01-27 12:00:00", tz="UTC"), 
    as.POSIXct("2020-01-27 12:01:00", tz="UTC")
)

# Beispieldaten laden
combined <- read_fst(
    "Cache/forex-combined/eurusd/tick/forex-combined-eurusd-tick-2020-01.fst",
    as.data.table=TRUE)
combined <- combined[Time %between% sampleTimeframe]
combined <- combined[1:9]

# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(combined))) {
    tick <- combined[i]
    printf("%s%s &\n",
           tabIndentFirst,
           formatPOSIXctWithFractionalSeconds(tick$Time, "%d.%m.%Y, %H:%M:%OS3")
    )
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$Bid, digits=5))
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$Ask, digits=5))
    printf("%s%s\\,USD &\n", tabIndent, format.money(tick$Mittel, digits=6))
    printf("%s%s \\\\\n", tabIndent, tick$Source)
    
    printf("\n")
}
