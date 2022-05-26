#' Kombiniert die (bereits als .fst vorliegenden) Tickdaten von
#' Dukascopy zu Stundendaten.


# Bibliotheken und Funktionen laden ---------------------------------------
library("fst")
library("data.table")
library("lubridate") # floor_date
library("tictoc")
source("Funktionen/AddOneMonth.R")
source("Funktionen/printf.R")


# Variablen initialisieren ------------------------------------------------

# Zeitraum: 01.01.2010 bis einschließlich letzten Monat
startDate <- as.POSIXct("2010-01-01")
endDate <- ((Sys.Date() |> format("%Y-%m-01") |> as.POSIXct()) - 1) |> 
    format("%Y-%m-01") |>
    as.POSIXct()
currentDate <- startDate - 1

# Zieldateien und Ergebnistabellen
# Gegenüber Bitcoin-Daten auch Stundendaten berechnen, um für die Auswertung
# die Bestimmung von handelsfreien Zeiten zu vereinfachen
targetFileHourly <- "Cache/dukascopy/eurusd/dukascopy-eurusd-hourly.fst"
dataset_hourly <- data.table()

# Hilfsfunktionen ---------------------------------------------------------

# Daten eines Zeitabschnittes aggregieren
summariseTickData <- function() {
    return(expression(.(
        CloseBid = last(Bid),
        CloseAsk = last(Ask),
        CloseMittel = last(Mittel),
        numDatasets = .N
    )))
}


# Daten kombinieren - derzeit nur EUR/USD ---------------------------------
while (currentDate < endDate) {
    currentDate <- addOneMonth(currentDate)
    printf("Verarbeite %02d/%d: ", month(currentDate), year(currentDate))
    
    # Quelldatei bestimmen
    sourceFileDukascopy <- sprintf(
        "Cache/dukascopy/eurusd/tick/dukascopy-eurusd-tick-%d-%02d.fst",
        year(currentDate), month(currentDate)
    )
    if (!file.exists(sourceFileDukascopy)) {
        printf("Quelldatei %s nicht gefunden.\n", sourceFileDukascopy)
        next
    }
    
    tic()
    dukascopy <- read_fst(sourceFileDukascopy, as.data.table=TRUE)
    
    # Auf 1h aggregieren (einzelne Datei für gesamten Datensatz)
    printf("1h... ")
    thisDataset_hourly <- dukascopy[
        j = eval(summariseTickData()),
        by = .(Time=floor_date(Time, unit="1 hour"))
    ]
    dataset_hourly <- rbindlist(list(dataset_hourly, thisDataset_hourly))
    
    toc()
}

# Daten speichern
write_fst(dataset_hourly, targetFileHourly, compress=100)
