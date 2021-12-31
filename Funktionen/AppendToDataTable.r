# Mittels reserviertem Speicher + `set` sehr viel schneller 
# Daten an eine data.table anfügen als mit := oder rbindlist
#
# In Anlehnung an https://stackoverflow.com/a/38052208
nrowDT <- function(dt) {
    n <- attr(dt, 'rowCount', exact=TRUE)
    if (is.null(n)) {
        n <- nrow(dt)
    }
    
    return(n)
}

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

# Reservierte, nicht genutzte Zeilen (`NAs`) wieder freigeben
cleanupDT <- function(dt, elems) {
    n <- attr(dt, 'rowCount', exact=TRUE)
    
    if (is.null(n)) {
        return(dt)
    }
    
    setattr(dt, "rowCount", NULL)
    return(dt[1:n,])
}
