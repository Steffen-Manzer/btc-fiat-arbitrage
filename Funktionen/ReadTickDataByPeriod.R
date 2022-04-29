# Benötigte Funktionen und Pakete laden
source("Funktionen/AddOneMonth.R")
source("Funktionen/LoadSuspicousBitcoinPeriods.R")
source("Funktionen/ReadDataFileChunked.R")
library("data.table")
library("lubridate") # is.POSIXct


#' Lade Tickdaten für das gesamte angegebene Intervall, ggf. über
#' mehrere Quelldateien hinweg
#' 
#' @param exchange Die gewünschte Börse / Datenquelle
#' @param currencyPair Der gewünschte Wechselkurs
#' @param startTime Zeitpunkt des aktuellen (zuletzt verwendeten) Ticks
#' @param endTime Zieldatum, bis zu dem mindestens gelesen werden soll
#' @param filterSuspiciousPeriods Auffällige Datensätze herausfiltern
#' @param ... Weitere Parameter, die an `readDataFileChunked` übergeben werden
#' @return `data.table` mit den Originaldaten
readTickDataByPeriod <- function(
    exchange,
    currencyPair,
    startTime,
    endTime,
    filterSuspiciousPeriods = FALSE,
    ...
) {
    
    # Parameter validieren
    stopifnot(
        is.character(exchange), length(exchange) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L,
        is.POSIXct(startTime), length(startTime) == 1L,
        is.POSIXct(endTime), length(endTime) == 1L,
        is.logical(filterSuspiciousPeriods), length(filterSuspiciousPeriods) == 1L
    )
    
    # Anomalien aus diesem Datensatz filtern
    if (filterSuspiciousPeriods == TRUE) {
        suspiciousPeriods <- loadSuspiciousPeriods(exchange, currencyPair)
    }
    
    # Beginne bei aktuellem Monat
    currentTime <- startTime
    result <- data.table()
    
    while (TRUE) {
        
        dataFile <- sprintf(
            "Cache/%s/%s/tick/%1$s-%2$s-tick-%3$d-%4$02d.fst",
            exchange, tolower(currencyPair), year(currentTime), month(currentTime)
        )
        if (!file.exists(dataFile)) {
            stop(sprintf("Datei nicht gefunden: %s", dataFile))
        }
        
        newData <- readDataFileChunked(dataFile, 1L, endTime, ...)
        
        # Auf gewünschten Zeitraum beschränken
        newData <- newData[Time %between% c(startTime, endTime)]
        if (nrow(newData) > 0L) {
            
            # Filtern
            if (filterSuspiciousPeriods == TRUE) {
                for (i in seq_len(nrow(suspiciousPeriods))) {
                    filtered <- newData[
                        Time %between% c(
                            suspiciousPeriods$startDate[i],
                            suspiciousPeriods$endDate[i]
                        ),
                        which=TRUE
                    ]
                    if (length(filtered) > 0) {
                        newData <- newData[!filtered]
                    }
                }
            }
            
            result <- rbindlist(list(result, newData), use.names=TRUE)
        }
        
        # Ziel-Monat erreicht, Ende.
        if (as.integer(format(endTime, "%Y%m")) <= as.integer(format(currentTime, "%Y%m"))) {
            break
        }
        
        # Lese zusätzlich nächsten Monat
        currentTime <- addOneMonth(currentTime)
    }
    
    return(result)
}
