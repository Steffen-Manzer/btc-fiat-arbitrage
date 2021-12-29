# Einfach ein Wrapper für prettyNum - für Zahlenausgabe, wie sie in
# Deutschland üblich ist: 123.456,789
numberFormat <- function(...) {
    prettyNum(..., big.mark=".", decimal.mark=",")
}
