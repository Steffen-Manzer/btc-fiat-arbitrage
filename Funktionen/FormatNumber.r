#' Zahlenausgabe, wie sie in DE üblich ist: 123.456,789
format.number <- function(...) {
    prettyNum(..., big.mark=".", decimal.mark=",")
}

#' Währungsausgabe, wie sie in DE üblich ist: 123456.789 -> 123.456,79
format.money <- function(..., digits=2) { 
    formatC(..., format="f", big.mark=".", decimal.mark=",", digits=digits)
}
