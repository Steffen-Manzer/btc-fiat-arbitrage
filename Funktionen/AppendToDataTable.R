#' Mittels reserviertem Speicher + `set` sehr viel schneller einzelne
#' Daten an eine data.table anfügen als mit := oder rbindlist
#'
#' In Anlehnung an https://stackoverflow.com/a/38052208


#' Echte Länge einer `data.table` berechnen, für die Speicher mittels
#' `appendDT` reserviert wurde
#' 
#' @param dt Eine `data.table`
#' @return `numeric` Anzahl der Datensätze in `dt`
nrowDT <- function(dt) {
    n <- attr(dt, 'rowCount', exact=TRUE)
    if (is.null(n)) {
        n <- nrow(dt)
    }
    
    return(n)
}


#' Eine Zeile an eine `data.table` anhängen
#' 
#' Reserviert vorab dynamisch Speicher für weitere Elemente und
#' weist dem so reservierten Platz das neue Element zu.
#' Dieses Verfahren ist bei wiederholter Anwendung weit schneller als
#' eine wiederholte Anwendung von `rbind` oder `rbindlist`.
#' 
#' @param dt Eine `data.table`
#' @param elems Eine Liste der neuen Zeile
#' @return `data.table` `dt`, um eine weitere Zeile mit `elems` ergänzt
appendDT <- function(dt, elems) {
    n <- nrowDT(dt)
    
    # Tabelle ist leer: Einfach neue Tabelle erstellen
    if (n == 0L) {
        return(as.data.table(elems))
    }
    
    # Letzte Zeile erreicht: Neuen Speicher reservieren
    if (n == nrow(dt)) {
        tmp <- elems[1]
        # Anzahl der Zeilen verdoppeln
        # TODO Verdoppelung nötig? Maximalzahl begrenzen?
        tmp[[1]] <- rep(NA, n)
        dt <- rbindlist(list(dt, tmp), fill=TRUE, use.names=TRUE)
        setattr(dt, 'rowCount', n)
    }
    
    pos <- as.integer(match(names(elems), colnames(dt)))
    for (j in seq_along(pos)) {
        set(dt, i=n+1L, pos[[j]], elems[[j]])
    }
    
    setattr(dt, 'rowCount', n+1L)
    
    return(dt)
}


#' Reservierte, nicht genutzte Zeilen (`NAs`) wieder freigeben
#' 
#' @param dt `data.table`, in der mittels `appendDT` Speicher reserviert wurde
#' @return `data.table` `dt`, um den reservierten Speicher bereinigt
cleanupDT <- function(dt) {
    n <- attr(dt, 'rowCount', exact=TRUE)
    
    # Keine 
    if (is.null(n)) {
        return(dt)
    }
    
    setattr(dt, "rowCount", NULL)
    return(dt[1:n])
}
