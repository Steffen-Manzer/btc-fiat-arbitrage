#' Zahlenausgabe, wie sie in DE üblich ist: 123.456,789
format.number <- function(...) {
    prettyNum(..., big.mark=".", decimal.mark=",", scientific=FALSE)
}

#' Ausgabe mit einer exakten Anzahl Nachkommastellen
format.numberWithFixedDigits <- function(..., digits=1L) { 
    formatC(..., format="f", big.mark=".", decimal.mark=",", digits=digits)
}

#' Ausgabe mit einer exakten Anzahl signifikanter Stellen
format.numberWithSignificantDigits <- function(n, digits) {
    formatC(signif(n, digits=digits), digits=digits, format="fg", flag="#", decimal.mark=",")
}

#' Währungsausgabe, wie sie in DE üblich ist: 123456.789 -> 123.456,79
format.money <- function(..., digits=2L) {
    formatC(..., format="f", big.mark=".", decimal.mark=",", digits=digits)
}

#' Prozentangabe mit fester Anzahl Nachkommastellen formatieren
format.percentage <- function(d, digits=3L) {
    return(formatC(d*100, digits=digits, format="f", decimal.mark=",", big.mark="."))
}
