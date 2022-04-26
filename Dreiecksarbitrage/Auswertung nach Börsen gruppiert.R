#' Auswertung der Dreiecksarbitrage an Bitcoin-Börsen
#'
#' Notwendig ist die vorherige Berechnung und Speicherung von Preistripeln
#' unter
#'   `Cache/Dreiecksarbitrage/{Börse}-{Währung 1}-{Währung 2}-{i}` mit `i = 1 ... n`.
#' über die Datei `Preistipel finden.R`.
#'
#' Stand Januar 2022 passen sämtliche Ergebnisse noch mit etwas Puffer in eine
#' einzelne Ergebnisdatei, ohne dass diese jeweils mehr als 4 GB Arbeitsspeicher
#' belegen würde.
#'
#' Diese Auswertung lädt aus diesem Grund derzeit je Kurs-/Börsenpaar nur die
#' erste Ergebnisdatei und prüft nicht, ob weitere Ergebnisse vorliegen.
#' Das Nachladen weiterer Ergebnisdateien müsste in Zukunft ergänzt werden, wenn
#' einzelne Dateien die Schwelle von 100 Mio. Datensätzen überschreiten.


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Klassen/TriangularResult.R")
source("Funktionen/DetermineCurrencyPairOrder.R")
source("Funktionen/FormatNumber.R")
#source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")
library("fst")
library("data.table")
#library("lubridate") # floor_date
library("ggplot2")
library("ggthemes")
library("scales") # breaks_extended



# Konfiguration -----------------------------------------------------------
exchanges <- list(
    "bitfinex"="Bitfinex",
    "bitstamp"="Bitstamp",
    "coinbase"="Coinbase Pro",
    "kraken"="Kraken"
)


# Hilfsfunktionen -------------------------------------------------------------

#' Ergebnis berechnen
#' 
#' Berechnet das Ergebnis der Dreiecksarbitrage für beide Forex-Richtungen:
#' Annahme: Bitcoin-Geld-/Briefkurse sind identisch, Devisen-Geld-/Brief nicht.
#' 
#' Daher werden für das Tripel BTC, EUR, USD folgende Routen durchgespielt:
#' Variante 1: EUR - BTC - USD - EUR (EUR-USD-Briefkurs)
#'     äq. zu: USD - EUR - BTC - USD
#' 
#' Variante 2: USD - BTC - EUR - USD (EUR-USD-Geldkurs)
#'     äq. zu: EUR - USD - BTC - EUR
#' 
#' @param data Eine Instanz der Klasse `TriangularResult` (per Referenz)
#' @return NULL (`data` wird per Referenz verändert)
calculateResult <- function(result)
{
    stopifnot(inherits(result, "TriangularResult"))
    
    # Vorliegenden Wechselkurs analysieren und Basis- und quotierte Währung bestimmen
    pair_a_b <- determineCurrencyPairOrder(result$Currency_A, result$Currency_B)
    baseFiatCurrency <- substr(pair_a_b, 1, 3)
    quotedFiatCurrency <- substr(pair_a_b, 4, 6)
    
    if (result$Currency_A == baseFiatCurrency && result$Currency_B == quotedFiatCurrency) {
        # A ist Basiswährung des Wechselkurses, Beispiel EUR/USD:
        # A = EUR = Basiswährung
        # B = USD = quotierte Währung
        # Umrechnung A -> B (Basis -> quotiert): *Bid (mit Bid multiplizieren)
        # Umrechnung B -> A (quotiert -> Basis): /Ask (durch Ask dividieren)
        a_to_b <- expr(ab_Bid)
        b_to_a <- expr(1/ab_Ask)
        
    } else if (result$Currency_B == baseFiatCurrency && result$Currency_A == quotedFiatCurrency) {
        # B ist Basiswährung des Wechselkurses, Beispiel EUR/USD:
        # A = USD = quotierte Währung
        # B = EUR = Basiswährung
        # Umrechnung A -> B (quotiert -> Basis): /Ask (durch Ask dividieren)
        # Umrechnung B -> A (Basis -> quotiert): *Bid (mit Bid multiplizieren)
        a_to_b <- expr(1/ab_Ask)
        b_to_a <- expr(ab_Bid)
        
    } else {
        stop("Hinterlegtes Wechselkurspaar ist nicht korrekt!")
    }
    
    # Ergebnisse berechnen (siehe Doku zu data.table: "set")
    result$data[, `:=`(
        # Route A->B: A -> B -> BTC -> A oder B -> BTC -> A -> B
        ResultAB = eval(a_to_b) * a_PriceHigh / b_PriceLow,
        
        # Route B->A: A -> BTC -> B -> A oder B -> A -> BTC -> B
        ResultBA = eval(b_to_a) * b_PriceHigh / a_PriceLow
    )]
    
    result$data[, BestResult := pmax(ResultAB, ResultBA)]
    
    return(invisible(NULL))
}


#' VERALTET: Verteilungsdichte zeichnen
#' 
#' @param route Tabelle mit Ergebnissen in der Spalte `Result`
#' @param removeOutliers Ausreißer entfernen (IQR-Methode)
#' @param plotTitle Plot-Titel
#' @param plotSubtitle Plot-Untertitel
#' @return ggplot-Objekt
plotDensity <- function(
        result,
        removeOutliers = TRUE,
        plotTitle = NA,
        plotSubtitle = NA
) {
    # Parameter validieren
    stopifnot(
        is.data.table(result), !is.null(result$Result),
        length(removeOutliers) == 1L, is.logical(removeOutliers),
        (is.na(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))),
        (is.na(plotSubtitle) || (length(plotSubtitle) == 1L && is.character(plotSubtitle)))
    )
    
    # Dichteplot ist im Grunde nur ohne Ausreißer interessant
    # TODO Ausreißer-Definition in Arbeit
    # Zitat aus Wikipedia:
    #' Die „Erwartung“ wird meistens als Streuungsbereich um
    #' den Erwartungswert herum definiert, in dem die meisten aller
    #' Messwerte zu liegen kommen, z. B. der Quartilabstand Q75 – Q25.
    #' Werte, die weiter als das 1,5-Fache des Quartilabstandes außerhalb
    #' dieses Intervalls liegen, werden (*meist willkürlich*) als Ausreißer bezeichnet.
    s <- summary(result$Result)
    if (removeOutliers) {
        iqr <- s[[5]] - s[[2]]
        lower <- max(s[[1]], s[[2]] - 1.5*iqr)
        upper <- min(s[[6]], s[[5]] + 1.5*iqr)
        xlim <- c(lower, upper)
        
        # n muss hier hoch sein, da Ausreißer ausgeblendet, nicht
        # jedoch aus der Berechnung entfernt werden
        # Sonst wäre die "reingezoomte" Dichte sehr grob
        #nDensity <- 2^14
        nDensity <- 2^16 # Bei mehreren Börsen
        
        # TODO Parameter from, to, cut in `density()`?!
        
    } else {
        xlim <- c(NA, NA)
        nDensity <- 2^9
    }
    
    p <- ggplot() +
        # stat_density mit geom="line" statt geom_density für eine bessere Legende
        # Siehe https://stackoverflow.com/a/17506172
        stat_density(
            aes(x=Result, colour=Exchange, group=Exchange, linetype=Exchange),
            data = result,
            geom = "line",
            position = "identity",
            n = nDensity
        ) +
        theme_minimal() +
        scale_x_continuous(
            labels = function(x) paste0(format.percentage(x - 1, 2L), " %"),
            breaks = breaks_extended(8)
        ) +
        scale_y_continuous(labels = format.number) +
        coord_cartesian(xlim=xlim) +
        scale_color_ptol() +
        scale_fill_ptol() +
        # TODO y-Achse korrekt: Dichte (density) oder Häufigkeit (in aes: y=after_stat(count))
        labs(
            x="Ergebnis", y="Dichte", 
            colour="Börse", linetype="Börse", 
            title=plotTitle, subtitle=plotSubtitle
        )
    return(p)
}


#' Groben Überblick in Konsole anzeigen
#' 
#' Gibt die Anzahl Datensätze insgesamt und > 1% / 0,5% / 0,2% / 0,1% an
#' 
#' @param result Eine Instanz der Klasse `TriangularResult`
showSummaryStatistics <- function(result)
{
    stopifnot(inherits(result, "TriangularResult"))
    
    numTotal <- nrow(result$data)
    printf("%s Datensätze, davon:\n", format.number(numTotal))
    for (largerThan in c(1, .5, .2, .1)) {
        numLarger <- length(result$data[BestResult >= (1 + largerThan / 100), which=TRUE])
        printf(
            "Ergebnis >= %.1f %%: %s (%s %%)\n",
            largerThan, format.number(numLarger), format.percentage(numLarger/numTotal)
        )
    }
}


#' Zusammenfassende Statistiken und Lagemaße berechnen
#' 
#' @param result Eine Instanz der Klasse `TriangularResult`
#' @return Ergebnisliste
calculateSummaryStatistics <- function(result)
{
    stopifnot(inherits(result, "TriangularResult"))
    
    return(list(
        exchange = result$ExchangeName,
        numRows = nrow(result$data),
        Max = max(result$data$BestResult),
        numRowsGt5 = length(result$data[BestResult >= (1 + 5/100), which=TRUE]),
        numRowsGt2 = length(result$data[BestResult >= (1 + 2/100), which=TRUE]),
        numRowsGt1 = length(result$data[BestResult >= (1 + 1/100), which=TRUE]),
        numRowsGtDot5 = length(result$data[BestResult >= (1 + .5/100), which=TRUE]),
        numRowsGtDot2 = length(result$data[BestResult >= (1 + .2/100), which=TRUE]),
        numRowsGtDot1 = length(result$data[BestResult >= (1 + .1/100), which=TRUE]),
        Q1 = quantile(result$data$BestResult, probs=.25, names=FALSE),
        Mean = mean(result$data$BestResult),
        Median = median(result$data$BestResult),
        Q3 = quantile(result$data$BestResult, probs=.75, names=FALSE)
    ))
}


# Haupt-Auswertungsfunktion ---------------------------------------------------

analyseTriangularArbitrage <- function(exchange, currency_a, currency_b)
{
    # Parameter validieren
    stopifnot(
        is.character(exchange), length(exchange) == 1L,
        is.character(currency_a), length(currency_a) == 1L, nchar(currency_a) == 3L,
        is.character(currency_b), length(currency_b) == 1L, nchar(currency_b) == 3L#,
        #length(removeOutliers) == 1L, is.logical(removeOutliers), !is.na(removeOutliers)
    )
    
    # Variablen initialisieren
    exchangeName <- exchanges[[exchange]]
    
    dataFile <- sprintf(
        "Cache/Dreiecksarbitrage 5s/%s-%s-%s-1.fst",
        exchange, currency_a, currency_b
    )
    stopifnot(file.exists(dataFile))
    
    # Daten einlesen
    printf("Verarbeite %s...\n", exchangeName)
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
    
    # Ergebnis der Arbitrage (beide Routen + Optimum) berechnen
    calculateResult(result)
    
    # Hier weiter...
}

# Bootstrap für Tests
exchange <- "coinbase"
currency_a <- "usd"
currency_b <- "eur"

# Auswertung händisch starten
if (FALSE) {
    
    analyseTriangularArbitrage("bitfinex", "usd", "eur")
    analyseTriangularArbitrage("bitstamp", "usd", "eur")
    analyseTriangularArbitrage("coinbase", "usd", "eur")
    analyseTriangularArbitrage("kraken", "usd", "eur")
    
}
