#' Erzeugt eine grafische Übersicht über die unterschiedlichen
#' Darstellungsarten, jeweils ohne Intervalle:
#' 
#' - Q1/Q3/Median


# Funktionen und Bibliotheken laden -------------------------------------------
source("Dreiecksarbitrage/Auswertung gruppiert.R")
source("Konfiguration/TikZ.R")


# Konfiguration ---------------------------------------------------------------

# LaTeX
plotAsLaTeX <- TRUE
texFileQ1Q3Median <- sprintf(
    "%s/Abbildungen/Dreiecksarbitrage/Methodik_Darstellungsarten_Q1Q3Median.tex",
    latexOutPath
)

# Beispieldaten und Achsenbeschriftung
exchange <- "kraken"
exchangeName <- "Kraken"
currency_a <- "usd"
currency_b <- "eur"
threshold <- 1L


# Rohdaten laden --------------------------------------------------------------
if (!exists("result") || !inherits(result, "TriangularResult")) {
    
    dataFile <- sprintf(
        "Cache/Dreiecksarbitrage/%ds/%s-%s-%s.fst",
        threshold, exchange, currency_a, currency_b
    )
    stopifnot(file.exists(dataFile))
    
    # Daten einlesen
    printf("Lese Daten für %s... ", exchangeName)
    result <- new(
        "TriangularResult",
        Exchange = exchange,
        ExchangeName = exchangeName,
        Currency_A = currency_a,
        Currency_B = currency_b,
        data = read_fst(
            dataFile,
            columns = c(
                "Time",
                "a_PriceLow", "a_PriceHigh", # z.B. BTC/USD
                "b_PriceLow", "b_PriceHigh", # z.B. BTC/EUR
                "ab_Bid", "ab_Ask" # z.B. EUR/USD
            ),
            as.data.table = TRUE
        )
    )
    printf("%s Datensätze.\n", format.number(nrow(result$data)))
    
    # Ergebnis der Arbitrage (beide Routen + Optimum) berechnen
    calculateResult(result)
    result$data[, Exchange:=exchangeName]
}


# Grafiken erstellen ----------------------------------------------------------

# Median
aggregatedResults <- aggregateResultsByTime(result$data, "1 month")
p_diff_median <- plotAggregatedResultsOverTime(aggregatedResults)
if (plotAsLaTeX) {
    tikz(
        file = texFileQ1Q3Median,
        width = documentPageWidth,
        height = 4 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}
print(p_diff_median)
if (plotAsLaTeX) {
    dev.off()
}
