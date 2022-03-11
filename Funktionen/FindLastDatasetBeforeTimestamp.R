#' Letzten Datensatz vor einem angegebenen Datum finden
#' 
#' @param d Eine `data.table` mit mind. der Spalte `Time`
#' @param before Ein POSIXct der Länge 1
#' @return Die Zeile aus `d`, die zeitlich am nächsten vor `before` liegt
findLastDatasetBeforeTimestamp <- function(d, before)
{
    # Angelehnt an "safeWhichMax" aus 
    # https://stackoverflow.com/a/71052939/3238708
    last <- which.min(d$Time <= before)
    
    # which.min gibt auch dann 1 zurück, wenn kein Datensatz passt
    if (last == 1L && d$Time[1] > before) {
        return(NA)
    }
    
    return(d[last])
}
