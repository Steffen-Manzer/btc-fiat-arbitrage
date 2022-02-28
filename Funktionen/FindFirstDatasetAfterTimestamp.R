#' Ersten Datensatz nach einem angegebenen Datum finden
#' 
#' @param d Eine `data.table` mit mind. der Spalte `Time`
#' @param after Ein POSIXct der Länge 1
findFirstDatasetAfterTimestamp <- function(d, after)
{
    # "safeWhichMax" aus https://stackoverflow.com/a/71052939/3238708
    first <- which.max(d$Time >= after)
    
    # which.max gibt auch dann 1 zurück, wenn kein Datensatz passt
    if (first == 1L && d$Time[1] < after) {
        return(NA)
    }
    
    return(d[first])
}
