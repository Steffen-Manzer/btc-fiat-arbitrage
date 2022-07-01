# Konfiguration ---------------------------------------------------------------
dataSource <- "Daten/coindesk/coindesk-bpi-close-60s.csv.gz"
dataTargetMonthly <- "Cache/coindesk/bpi-monthly-btcusd.fst"
dataTarget1d <- "Cache/coindesk/bpi-daily-btcusd.fst"
dataTarget60s <- "Cache/coindesk/bpi-60s-btcusd.fst"

# Nur einmal pro Monat neu laden
if (file.exists(dataTarget60s) && difftime(Sys.time(), file.mtime(dataTarget60s), units = "days") < 28) {
    cat("BPI-Daten noch aktuell, keine Aktualisierung.\n")
    return()
}

# Verzeichnis anlegen
if (!dir.exists("Cache/coindesk")) {
    dir.create("Cache/coindesk")
}


# Bibliotheken laden ----------------------------------------------------------
library("fst")
library("data.table")
library("lubridate") # floor_date
library("fasttime") # fastPOSIXct
library("tictoc")


# Lese Daten: BPI Tick = 60s --------------------------------------------------
cat("Reading CSV... ")
tic()
bpi.60s <- fread(dataSource, header=TRUE, colClasses=c("character", "numeric"))
toc()

# Zeit parsen, UTC
cat("Parsing as 60s data and saving as .fst ... ")
tic()
bpi.60s$Time <- fastPOSIXct(bpi.60s$Time)

# Mögliche Duplikate entfernen
bpi.60s <- bpi.60s[j = .(Close = last(Close)), by=Time]

# Rendite berechnen
bpi.60s$Rendite <- c(0, diff(log(bpi.60s$Close)))

# Speichern
write_fst(bpi.60s, dataTarget60s)
toc()


# Aggregieren auf Tagesdaten --------------------------------------------------
cat("Aggregating as daily data and saving as .fst ... ")
tic()
bpi.perDay <- bpi.60s[
    j = .(
        Mean = mean(Close),
        Open = first(Close),
        High = max(Close),
        Low = min(Close),
        Close = last(Close), # Muss bei gleichem Namen die letzte Zeile sein
        NumDatasets = .N
    ),
    by = .(Time = as.Date(Time))
]

# Rendite
bpi.perDay$Rendite <- c(0, diff(log(bpi.perDay$Close)))

# Speichern
write_fst(bpi.perDay, dataTarget1d, compress=100L)
toc()


# Aggregieren auf Monatsdaten -------------------------------------------------
cat("Aggregating as monthly data and saving as .fst ... ")
tic()
bpi.perMonth <- bpi.60s[
    j = .(
        Mean = mean(Close),
        Open = first(Close),
        High = max(Close),
        Low = min(Close),
        Close = last(Close), # Muss bei gleichem Namen die letzte Zeile sein
        NumDatasets = .N
    ),
    by = .(Time = as.Date(floor_date(Time, "month")))
]

# Rendite
bpi.perMonth$Rendite <- c(0, diff(log(bpi.perMonth$Close)))

# Speichern
write_fst(bpi.perMonth, dataTargetMonthly, compress=100L)
toc()
