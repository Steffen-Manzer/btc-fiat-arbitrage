#' Kombiniert die (bereits als .fst vorliegenden) Tickdaten von TrueFX und
#' Dukascopy zu einem einzelnen Datensatz, der entsprechend einen größeren 
#' Zeitraum abdecken kann und somit für den Vergleich gegen Bitcoin-Börsen
#' mehr Ergebnisse liefert.


# Bibliotheken und Funktionen laden ---------------------------------------
library("fst")
library("data.table")
library("lubridate")
source("Funktionen/AddOneMonth.r")
source("Funktionen/printf.r")


# Variablen initialisieren ------------------------------------------------

# Zeitraum: 01.01.2010 bis einschließlich letzten Monat
startDate <- as.POSIXct("2010-01-01")
endDate <- ((Sys.Date() |> format("%Y-%m-01") |> as.POSIXct()) - 1) |> 
    format("%Y-%m-01") |>
    as.POSIXct()
currentDate <- startDate - 1

# Zieldateien und Ergebnistabellen
targetFileDaily <- "Cache/forex-combined/eurusd/forex-combined-eurusd-daily.fst"
targetFileMonthly <- "Cache/forex-combined/eurusd/forex-combined-eurusd-monthly.fst"
dataset_daily <- data.table()
dataset_monthly <- data.table()


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
    
    
    # Quelldateien bestimmen
    sourceFileTrueFX <- sprintf(
        "Cache/truefx/eurusd/tick/truefx-eurusd-tick-%d-%02d.fst",
        year(currentDate), month(currentDate)
    )
    sourceFileDukascopy <- sprintf(
        "Cache/dukascopy/eurusd/tick/dukascopy-eurusd-tick-%d-%02d.fst",
        year(currentDate), month(currentDate)
    )
    
    if (!file.exists(sourceFileTrueFX) || !file.exists(sourceFileDukascopy)) {
        printf("Eine der beiden Dateien nicht gefunden.\n")
        next
    }
    
    
    # Zieldateien festlegen
    targetFileTick <- sprintf(
        "Cache/forex-combined/eurusd/tick/forex-combined-eurusd-tick-%d-%02d.fst",
        year(currentDate), month(currentDate)
    )
    targetFile60s <- sprintf(
        "Cache/forex-combined/eurusd/60s/forex-combined-eurusd-60s-%d-%02d.fst",
        year(currentDate), month(currentDate)
    )
    
    for (targetPath in c(targetFileTick, targetFile60s)) {
        if (!file.exists(dirname(targetPath))) {
            dir.create(dirname(targetPath), recursive = TRUE)
        }
    }
    
    
    # TrueFX einlesen
    truefx <- read_fst(sourceFileTrueFX, as.data.table = TRUE)
    truefx[, Source:="TrueFX"]
    
    dukascopy <- read_fst(sourceFileDukascopy, as.data.table = TRUE)
    dukascopy[, Source:="Dukascopy"]
    
    combined <- rbindlist(list(truefx, dukascopy))
    rm(truefx, dukascopy)
    gc()
    
    # Sortieren
    setorder(combined, Time)
    
    # Speichern
    write_fst(combined, targetFileTick, compress=100)
    
    # Auf 60s aggregieren
    if (!file.exists(targetFile60s)) {
        printf(", 60s")
        thisDataset_60s <- combined[
            j=eval(summariseTickData()),
            by=floor_date(Time, unit = "minute")
        ]
        setnames(thisDataset_60s, 1, "Time")
        write_fst(thisDataset_60s, targetFile60s, compress=100)
        
        # Speicher freigeben
        rm(thisDataset_60s)
    }
    
    # Auf 1d aggregieren (einzelne Datei für gesamten Datensatz)
    printf(", 1d ")
    thisDataset_daily <- combined[
        j=eval(summariseTickData()),
        by=floor_date(Time, unit = "1 day")
    ]
    setnames(thisDataset_daily, 1, "Time")
    dataset_daily <- rbindlist(list(dataset_daily, thisDataset_daily))
    write_fst(dataset_daily, targetFileDaily, compress=100)
    rm(thisDataset_daily)
    
    # Auf 1 Monat aggregieren (einzelne Datei für gesamten Datensatz)
    printf(", 1mo ")
    thisDataset_monthly <- combined[
        j=eval(summariseTickData()),
        by=floor_date(Time, unit = "1 month")
    ]
    setnames(thisDataset_monthly, 1, "Time")
    dataset_monthly <- rbindlist(list(dataset_monthly, thisDataset_monthly))
    write_fst(dataset_monthly, targetFileMonthly, compress=100)
    
    rm(thisDataset_monthly, combined)
    printf("\n")
    gc()
}
