# Benötigt `readDataFileChunked`
source("Funktionen/ReadDataFileChunked.r")

#' Lade Tickdaten für das gesamte angegebene Intervall, ggf. über
#' mehrere Quelldateien hinweg
#' 
#' Das gewählte Intervall darf dabei einen ganzen Monat (28-31 Tage) 
#' nicht überschreiten. Daten bis zwei Minuten vor `currentTime` werden
#' entfernt, um Speicher freizugeben.
#' 
#' @param dataset Eine Instanz der Klasse `Dataset`
#' @param currentTime Zeitpunkt des aktuellen (zuletzt verwendeten) Ticks
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @return `NULL` (Verändert den angegebenen Datensatz per Referenz.)
readAndAppendNewTickData <- function(dataset, currentTime, endDate) {
    
    # Parameter validieren
    stopifnot(
        inherits(dataset, "Dataset"),
        is.POSIXct(currentTime),
        is.POSIXct(endDate)
    )
    
    numNewRows <- 0L
    if (nrow(dataset$data) > 0L) {
        
        # Speicherbereinigung: Bereits verarbeitete Daten löschen
        # Einen Zeitraum von wenigen Minuten vor dem aktuell 
        # betrachteten Tick beibehalten.
        # `data.table` kann leider noch kein subsetting per Referenz, sodass eine
        # Kopie (`<-`) notwendig ist.
        printf.debug("Bereinige Daten vor %s.\n", format(currentTime - 2 * 60))
        dataset$data <- dataset$data[Time >= (currentTime - 2 * 60),]
        
        # Lese Daten ab dem letzten Tick ein
        currentTime <- last(dataset$data$Time)
        startRow <- last(dataset$data$RowNum)
        
        # Bereits Daten bis über das angegebene Enddatum hinaus eingelesen
        if (currentTime > endDate) {
            return(invisible())
        }
        
    } else {
        
        # Ab erster Zeile starten
        startRow <- 1L
        
    }
    
    while (TRUE) {
        
        # Beginne immer bei aktuellem Monat
        dataFile <- sprintf(
            "%s-%d-%02d.fst",
            dataset$PathPrefix, year(currentTime), month(currentTime)
        )
        if (!file.exists(dataFile)) {
            stop(sprintf("Datei nicht gefunden: %s", dataFile))
        }
        
        printf.debug("Lese %s ab Zeile %s bis ", basename(dataFile), startRow |> format.number())
        newData <- readDataFileChunked(dataFile, startRow, endDate)
        numNewRows <- numNewRows + nrow(newData)
        
        # Letzte Zeile nur für Debug-Zwecke speichern
        if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
            lastRowNumber <- last(newData$RowNum)
            if (is.null(lastRowNumber)) {
                lastRowNumber <- startRow
            }
        }
        
        if (numNewRows > 0L) {
            # Börse hinterlegen und an bestehende Daten anfügen
            newData[, Exchange:=dataset$Exchange]
            printf.debug("%s (von %s): %s Datensätze.\n",
                         lastRowNumber |> format.number(),
                         metadata_fst(dataFile)$nrOfRows |> format.number(),
                         numNewRows |> format.number()
            )
            dataset$data <- rbindlist(list(dataset$data, newData), use.names=TRUE)
        } else {
            printf.debug("%s: Keine neuen Datensätze.\n", lastRowNumber |> format.number())
        }
        
        # Zieldatum erreicht und mehr als 100 Datensätze geladen:
        # Keine weiteren Daten laden.
        if (
            as.integer(format(endDate, "%Y%m")) <= as.integer(format(currentTime, "%Y%m")) &&
            numNewRows > 100L
        ) {
            break
        }
        
        # Lese zusätzlich nächsten Monat
        currentTime <- addOneMonth(currentTime)
        startRow <- 1L
        
        # Nächster Monat liegt außerhalb des verfügbaren Datenbereichs, abbrechen.
        if (currentTime > dataset$EndDate) {
            break
        }
    }
    
    return(invisible())
}
