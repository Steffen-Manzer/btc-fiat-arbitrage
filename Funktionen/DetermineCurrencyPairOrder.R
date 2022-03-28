#' Richtige Kurspaar-Benennung aus zwei Währungen bestimmen
#' 
#' @param currency_a Währung 1 (z.B. btc)
#' @param currency_a Währung 2 (z.B. usd)
#' @return Korrekte Kurspaar-Bezeichnung, z.B. btcusd
determineCurrencyPairOrder <- function(currency_a, currency_b)
{
    # Für die bis dato genutzten Datensätze ist die alphabetische
    # Richtung stets korrekt:
    # BTCEUR, BTCUSD, EURUSD
    # Potentiell ebenfalls korrekt, aber derzeit nicht genutzt:
    # BTCUSD, BTCGBP, GBPUSD
    # BTCEUR, BTCGBP, EURGBP
    # BTCJPY, BTCGBP, GBPJPY
    if (currency_a < currency_b) {
        return(paste0(currency_a, currency_b))
    } else {
        return(paste0(currency_b, currency_a))
    }
}
