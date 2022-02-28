#' Formatiert ein Währungspaar für eine Ausgabe in Statistiken, Tabellen, u.a.
#' 
#' @param pair Ein sechsstelliges Kurspaar, bspw. 'btcusd'
#' @return Formatiertes `pair`, bspw. BTC/USD
format.currencyPair <- function(pair) {
    stopifnot(is.character(pair), length(pair) == 1L, nchar(pair) == 6L)
    pair <- toupper(pair)
    return(paste0(substr(pair, 0, 3), "/", substr(pair, 4, 6)))
}
