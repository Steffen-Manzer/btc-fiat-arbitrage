#' Intervalle mit den angegebenen Breakpoints berechnen
calculateIntervals <- function(timeBoundaries, breakpoints)
{
    # Parameter validieren
    stopifnot(
        length(timeBoundaries) > 1L, (is.POSIXct(timeBoundaries) || is.Date(timeBoundaries)),
        length(breakpoints) > 0L
    )
    
    breakpoints <- as.POSIXct(breakpoints)
    intervals <- data.table()
    
    # Erstes Intervall
    prevDate <- as.Date(min(timeBoundaries))
    for (i in seq_along(breakpoints)) {
        intervals <- rbindlist(list(intervals, data.table(
            From = c(prevDate),
            To = c(as.Date(breakpoints[i] - 1)),
            Set = c(as.character(i))
        )))
        prevDate <- as.Date(breakpoints[i])
    }
    
    # Letztes Intervall
    intervals <- rbindlist(list(intervals, data.table(
        From = c(prevDate),
        To = c(as.Date(max(timeBoundaries))),
        Set = c(as.character(i+1))
    )))
    
    intervals$From <- as.POSIXct(intervals$From)
    intervals$To <- as.POSIXct(intervals$To)
    
    return(intervals)
}
