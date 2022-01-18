#' Den zeitlich nächstgelegenen Datensatz zum angegebenen Datum finden
#' 
#' Findet den zeitlich am nächsten liegenden Datenpunkt innerhalb der
#' vorgegebenen Zeitspanne (+/-) im angegebenen Datensatz.
#' 
#' Die Funktion ist nicht auf Geschwindigkeit optimiert und daher nicht für die
#' Anwendung innerhalb einer Schleife für große Datensätze geeignet.
#' 
#' @author Steffen Manzer
#' @param datetime Gesuchter Zeitpunkt.
#' @param dataset Datensatz als data.table/data.frame.
#'   Die Zeit muss in der Spalte "Time" hinterlegt sein.
#' @param threshold Maximale Differenz zum gesuchten Zeitpunkt in Sekunden.
#' @param withPastData Auch vergangene Zeitpunkte berücksichtigen.
#' @param getAllWithinThreshold Alle Datensätze innerhalb des gegebenen 
#'   Limits (nach zeitlicher Nähe sortiert) zurückgeben.
#'   Nützlich, um später detaillierter zu filtern.
#' @return Nächstgelegener Datensatz, Liste aller Datensätze
#'   innerhalb von threshold oder NA, falls innerhalb von threshold
#'   kein Datensatz existiert.
#' @examples
#' # NOT RUN {
#' findNearestDatapoint(
#'   datetime = as.POSIXct("2021-12-05 19:35:00.282"), 
#'   dataset = bitstamp_tick_data
#' )
#' # }
findNearestDatapoint <- function(
    datetime,
    dataset,
    threshold = 5,
    withPastData = TRUE,
    getAllWithinThreshold = FALSE
) {
    # Bibliotheken laden
    library("lubridate") # is.POSIXct
    
    # Parameter validieren
    stopifnot(
        length(datetime) == 1,
        is.POSIXct(datetime),
        !is.null(dataset$Time),
        is.POSIXct(dataset$Time),
        length(threshold) == 1,
        is.numeric(threshold)
    )
    
    # Optional: Daten vor dem gesuchten Zeitpunkt entfernen
    if (isFALSE(withPastData)) {
        dataset <- dataset[dataset$Time >= datetime,]
        if (nrow(dataset) == 0) {
            return(NA)
        }
    }
    
    # Daten außerhalb des konfigurierten Zeitfensters entfernen
    dataset <- dataset[abs(dataset$Time - datetime) <= threshold,]
    if (nrow(dataset) == 0) {
        return(NA)
    }
    
    # Nach zeitlicher Nähe sortieren und im Datensatz hinterlegen
    dataset$TimeDifference <- abs(dataset$Time - datetime)
    setorder(dataset, TimeDifference)
    #dataset <- dataset[order(abs(dataset$TimeDifference)),]
    
    if (isTRUE(getAllWithinThreshold)) {
        # Alle gefundenen Datensätze nach zeitlicher Nähe sortiert zurückgeben
        return(dataset)
        
    } else {
        # Nur den zeitlich nächsten Datensatz zurückgeben
        return(dataset[1,])
        
    }
}
