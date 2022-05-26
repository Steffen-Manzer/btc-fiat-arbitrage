library("fst")
library("data.table")
library("zoo") # rollapply
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/printf.R")

# Betrachtetes Zeitfenster
# Unterscheidet sich von der Raumarbitrage, da bei dem dortigen Zeitfenster
# kein Devisenhandel stattfand
timeframe <- c("2021-12-05 23:06:00.9", "2021-12-05 23:06:02.176700")

# Beispieldaten laden
forex <- read_fst(
    "Cache/forex-combined/eurusd/tick/forex-combined-eurusd-tick-2021-12.fst",
    columns = c("Time", "Bid", "Ask", "Mittel"),
    as.data.table = TRUE
)

# Auf Beispiel-Zeitfenster beschränken
forex <- forex[Time %between% timeframe]

# Als LaTeX-Tabelle ausgeben
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(forex))) {
    tick <- forex[i]
    printf(
        "%s%s &\n",
        tabIndentFirst, formatPOSIXctWithFractionalSeconds(tick$Time, "%d.%m.%Y, %H:%M:%OS")
    )
    printf(
        "%s%s\\,%s &\n",
        tabIndent, format.money(tick$Bid, digits=5), "USD"
    )
    printf(
        "%s%s\\,%s &\n",
        tabIndent, format.money(tick$Ask, digits=5), "USD"
    )
    printf(
        "%s%s\\,%s \\\\\n",
        tabIndent, format.money(tick$Mittel, digits=6), "USD"
    )
    
    printf("\n")
}
