#' Ein Ergebnisdatensatz der Dreiecksarbitrage.
#'
#' Definiert als eigene Klasse, die per Referenz übergeben und somit
#' bei Bedarf in-place bearbeitet werden kann.
library("data.table")
setRefClass(
    "TriangularResult", 
    fields = list(
        
        # Börse in Kleinbuchstaben, z.B. `bitfinex`
        Exchange = "character",
        
        # Börsenname, ausgeschrieben
        ExchangeName = "character",
        
        # Währung 1 und 2 in Kleinbuchstaben, z.B. `usd` oder `eur`
        Currency_A = "character",
        Currency_B = "character",
        
        # Preis- und Ergebnistabelle: Enthält (min.) die Spalten
        # `Time`: Zeitstempel
        # `a_PriceLow`, `a_PriceHigh`: Tiefst-/Höchstpreis BTC/A, z.B. BTC/USD
        # `b_PriceLow`, `b_PriceHigh`: Tiefst-/Höchstpreis BTC/B, z.B. BTC/EUR
        # `ab_Bid`, `ab_Ask`: Devisenkurs, z.B. EUR/USD
        #     Anm.: Sinnvoller wäre die Bezeichnung forex_Bid und forex_Ask gewesen,
        #     da die Reihenfolge nicht zwingend ab (also USD/EUR vs. EUR/USD) ist.
        #     Aufgrund der bereits berechneten Bestandsdaten ist eine nachträgliche
        #     Änderung jedoch nicht ohne weiteres Möglich.
        data = "data.table"
        
    )
)
