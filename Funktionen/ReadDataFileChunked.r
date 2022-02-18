# Benötigte Bibliotheken laden
library("fst")
library("data.table")

#' Liest eine einzelne angegebene .fst-Datei bis zum Dateiende oder 
#' bis endDate, je nachdem was früher eintritt
#' 
#' Dabei wird geprüft, ob mehrere Ticks zum selben Zeitpunkt auftreten.
#' Gegebenenfalls werden weitere Daten geladen, bis alle Ticks des letzten
#' Zeitpunktes im Datensatz enthalten sind.
#' 
#' @param dataFile Absoluter Pfad zu einer .fst-Datei
#' @param startRow Zeilennummer, ab der gelesen werden soll
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @param numDatasetsPerRead Datensätze, die an einem Stück gelesen werden,
#'   bevor geprüft wird, ob ein Abbruchkriterium erreicht wurde
#' @return `data.table` mit den gelesenen Daten (`Time`, `Price`, `RowNum`)
readDataFileChunked <- function(
    dataFile,
    startRow,
    endDate,
    numDatasetsPerRead = 10000L
) {
    
    # Umgebungsbedingungen prüfen
    stopifnot(
        is.integer(numDatasetsPerRead), length(numDatasetsPerRead) == 1L,
        file.exists(dataFile)
    )
    
    # Ergebnis aufbauen
    result <- data.table()
    
    # Metadaten der Datei lesen
    numRowsInFile <- metadata_fst(dataFile)$nrOfRows
    columns <- c("ID", "Time", "Price")
    
    # Keine weiteren Daten in dieser Datei: Abbruch
    if (startRow == numRowsInFile) {
        return(result)
    }
    
    # Lese Daten iterativ ein, bis ein Abbruchkriterium erfüllt ist
    while (TRUE) {
        
        # Limit bestimmen: `numDatasetsPerRead` Datensätze, maximal bis zum Ende der Datei
        endRow <- min(numRowsInFile, startRow + numDatasetsPerRead - 1L)
        
        # Datei einlesen
        # printf.debug("Lese %s von Zeile %d bis %d: ", basename(dataFile), startRow, endRow)
        newData <- read_fst(dataFile, columns, startRow, endRow, as.data.table=TRUE)
        newData[, RowNum:=startRow:endRow]
        
        # Daten anhängen
        if (nrow(result) > 0) {
            result <- rbindlist(list(result, newData), use.names=TRUE)
        } else {
            result <- newData
        }
        # printf.debug("%d weitere Datensätze, %d insgesamt.\n", nrow(newData), nrow(result))
        
        # Dateiende wurde erreicht
        # Da die Daten monatsweise sortiert sind, ist der nächste Tick
        # immer zu einem anderen Zeitpunkt. Eine Prüfung, ob der nächste
        # Tick die selbe Zeit aufweist, ist hier also nicht erforderlich
        if (endRow == numRowsInFile) {
            break
        }
        
        # endDate wurde erreicht: Prüfe zusätzlich, ob noch weitere
        # Ticks mit der exakt selben Zeit vorliegen und lade alle
        # solchen Ticks, sonst kommt es zu Fehlern in der Auswertung
        lastTime <- last(result$Time)
        if (lastTime > endDate) {
            
            # Prüfe weitere Ticks nur, solange Dateieende nicht erreicht wurde
            while (endRow < numRowsInFile) {
                
                endRow <- endRow + 1L
                oneMoreRow <- read_fst(
                    dataFile, columns, endRow, endRow, as.data.table=TRUE
                )
                
                # Nächster Tick ist nicht in der selben Sekunde:
                # Einlesen abgeschlossen.
                if (oneMoreRow$Time[1] > lastTime) {
                    break
                }
                
                # Nächster Tick ist in der exakt selben Sekunde: anhängen.
                oneMoreRow[, RowNum:=endRow]
                result <- rbindlist(list(result, oneMoreRow), use.names=TRUE)
            }
            
            # Fertig.
            break
        }
        
        
        # Weitere `numDatasetsPerRead` Datenpunkte lesen
        startRow <- startRow + numDatasetsPerRead
    }
    
    return(result)
}
