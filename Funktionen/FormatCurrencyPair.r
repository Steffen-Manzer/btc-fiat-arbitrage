# Formatiert ein Währungspaar etwas schöner: Großbuchstaben und Schrägstrich.
# btcusd -> BTC/USD
format.currencyPair <- function(pair) {
    stopifnot(is.character(pair), length(pair) == 1L, nchar(pair) == 6L)
    pair <- toupper(pair)
    return(paste0(substr(pair, 0, 3), "/", substr(pair, 4, 6)))
}
