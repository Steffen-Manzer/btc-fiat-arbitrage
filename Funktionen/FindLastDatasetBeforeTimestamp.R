#' Letzten Datensatz vor einem angegebenen Datum finden
#' 
#' @param d Eine nach Zeit sortierte `data.table` mit der Spalte `Time`
#' @param before Ein POSIXct der Länge 1
#' @return Die Zeile aus `d`, die zeitlich am nächsten vor `before` liegt
findLastDatasetBeforeTimestamp <- function(d, before)
{
    # Alle Daten liegen vor dem angegebenen Datum
    if (last(d$Time) <= before) {
        return(last(d))
    }
    
    # Alle Daten liegen nach dem angegebenen Datum
    if (d$Time[1] > before) {
        return(NA)
    }
    
    # Angelehnt an https://stackoverflow.com/a/71052939/3238708
    # Da die Grenzen oben bereits geprüft wurden, gibt which.min hier immer
    # einen Wert zwischen 2 und n zurück.
    last <- which.min(d$Time <= before) - 1L
    
    return(d[last])
}
