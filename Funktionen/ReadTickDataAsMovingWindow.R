# Benötigte Funktionen und Pakete laden
source("Funktionen/AddOneMonth.R")
source("Funktionen/ReadDataFileChunked.R")
library("data.table")
library("lubridate") # is.POSIXct, floor_date


#' Lade Tickdaten für Auswertungen, die ein gleitendes Fenster verwenden.
#' 
#' Daten werden bis `endDate` in `dataset` gelesen und dort vorhandene Daten
#' bis zwei Minuten vor `currentTime` entfernt, um nicht benötigten
#' Speicher freizugeben.
#' 
#' @param dataset Eine Instanz der Klasse `Dataset` (als Referenz)
#' @param currentTime Zeitpunkt des aktuellen (zuletzt verwendeten) Ticks
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @param ... Weitere Parameter, die an `readDataFileChunked` übergeben werden
#' @return `NULL` (Verändert den angegebenen Datensatz per Referenz.)
readTickDataAsMovingWindow <- function(dataset, currentTime, endDate, ...)
{
    # Parameter validieren
    stopifnot(
        inherits(dataset, "Dataset"),
        is.POSIXct(currentTime), length(currentTime) == 1L,
        is.POSIXct(endDate), length(endDate) == 1L
    )
    
    numNewRows <- 0L
    if (nrow(dataset$data) > 0L) {
        
        # Speicherbereinigung: Bereits verarbeitete Daten löschen
        # Einen Zeitraum von wenigen Minuten vor dem aktuell 
        # betrachteten Tick beibehalten.
        # `data.table` kann leider noch kein subsetting per Referenz, sodass eine
        # Kopie (`<-`) notwendig ist.
        currentTime <- min(currentTime, last(dataset$data$Time))
        
        printf.debug("Bereinige Daten vor %s.\n", format(currentTime - 2 * 60))
        dataset$data <- dataset$data[Time >= (currentTime - 2 * 60)]
        
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
    
    # Falls relevant: Handelszeiten eines "verknüpften"
    # Wechselkursdatensatzes berücksichtigen und auf diese
    # Zeiten begrenzen
    if (!is.null(dataset$TradingHours)) {
        
        # Ende bestimmen
        endHour <- floor_date(endDate, unit="1 hour")
        
        # Gewünschtes Ende liegt außerhalb der Handelszeiten -> Nächsten
        # Zeitpunkt innerhalb der Handelszeiten bestimmen.
        # Das verhindert, dass lediglich solche neue Daten geladen werden,
        # die letztlich verworfen werden
        if (!(endHour %in% dataset$TradingHours)) {
            tradingHoursAfterEndDate <- which(dataset$TradingHours > endDate)
            
            # Enddatum nur anpassen, wenn weitere Daten innerhalb der
            # Handelszeiten verfügbar sind, sonst einfach Rest lesen
            if (length(tradingHoursAfterEndDate) > 0L) {
                endHour <- dataset$TradingHours[tradingHoursAfterEndDate[1]]
            }
        }
        
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
        numRowsInFile <- metadata_fst(dataFile)$nrOfRows
        
        printf.debug("Lese %s ab Zeile %s bis ", basename(dataFile), startRow |> format.number())
        newData <- readDataFileChunked(dataFile, startRow, endDate, ...)
        lastRowNumber <- last(newData$RowNum)
        if (is.null(lastRowNumber)) {
            lastRowNumber <- startRow
        }
        
        # Filtern
        if (!is.null(dataset$SuspiciousPeriods) && nrow(newData) > 0L) {
            for (i in seq_len(nrow(dataset$SuspiciousPeriods))) {
                filtered <- newData[
                    Time %between% c(
                        dataset$SuspiciousPeriods$startDate[i],
                        dataset$SuspiciousPeriods$endDate[i]
                    ),
                    which=TRUE
                ]
                if (length(filtered) > 0) {
                    newData <- newData[!filtered]
                }
            }
        }
        
        # Auf Handelszeiten beschränken
        if (!is.null(dataset$TradingHours) && nrow(newData) > 0L) {
            newData <- newData[floor_date(Time, unit="1 hour") %in% dataset$TradingHours]
        }
        
        numNewRows <- numNewRows + nrow(newData)
        
        if (numNewRows > 0L) {
            # Börse und Kurs hinterlegen und an bestehende Daten anfügen
            if (length(dataset$Exchange) == 1L) {
                newData[, Exchange:=dataset$Exchange]
            }
            if (length(dataset$CurrencyPair) == 1L) {
                newData[, CurrencyPair:=dataset$CurrencyPair]
            }
            printf.debug(
                "%s (von %s): %s Datensätze.\n",
                lastRowNumber |> format.number(),
                numRowsInFile |> format.number(),
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
        
        # Noch weitere Daten im aktuellen Monat
        if (numRowsInFile > lastRowNumber) {
            startRow <- lastRowNumber + 1L
            next
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
