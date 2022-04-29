# Notwendige Bibliotheken laden
library("rjson")

#' Lade Zeiträume mit erkannten Ausreißern / Börsenfehlfunktionen
#' 
#' @param exchange Name der Bitcoin-Börse
#' @param currencyPair Kurspaar, z.B. btcusd
#' @return `data.table` Tabelle mit den Spalten `startDate` und `endDate`
loadSuspiciousPeriods <- function(exchange, currencyPair)
{
    # Metadaten einlesen
    allExchangeMetadata <- fromJSON(file="Daten/bitcoin-metadata.json")
    
    # Börse heraussuchen
    exchangeMetadata <- allExchangeMetadata[[exchange]]
    stopifnot(!is.null(exchangeMetadata))
    
    # Kurspaar an dieser Börse heraussuchen
    pairMetadata <- exchangeMetadata[[currencyPair]]
    stopifnot(!is.null(pairMetadata))
    
    # Ergebnistabelle erstellen
    suspiciousPeriods <- data.table()
    for (i in seq_along(pairMetadata$suspiciousPeriods)) {
        period <- pairMetadata$suspiciousPeriods[[i]]
        if (isFALSE(period$filter)) {
            next
        }
        suspiciousPeriods <- rbind(suspiciousPeriods, data.table(
            startDate = as.POSIXct(period$startDate, tz="UTC"),
            endDate = as.POSIXct(period$endDate, tz="UTC")
        ))
    }
    
    return(suspiciousPeriods)
}
