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
source("Funktionen/CalculateIntervals.R")
source("Funktionen/DetermineCurrencyPairOrder.R")
source("Funktionen/FormatCurrencyPair.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")
library("fst")
library("data.table")
library("lubridate") # floor_date
library("ggplot2")
library("ggthemes") # Einfaches Farbschema von Paul Tol
library("khroma") # Weitere, detailliertere Farbschemata von Paul Tol
library("cowplot") # plot_grid
library("readr") # read_file, write_file
#library("scales") # breaks_extended
library("stringr") # str_replace
library("tictoc")
library("TTR") # volatility


# Konfiguration -----------------------------------------------------------
plotAsLaTeX <- FALSE
exchangeNames <- list(
    "bitfinex" = "Bitfinex",
    "bitstamp" = "Bitstamp",
    "coinbase" = "Coinbase Pro",
    "kraken" = "Kraken"
)

# Verfügbare Grenzwerte für Abschluss des Arbitragegeschäfts in Sekunden
thresholds <- c(2L, 5L, 10L)

# Haupt-Intervall zur Betrachtung innerhalb der Arbeit
# (Rest: Nur Anhang, reduzierte Ansicht)
mainThreshold <- 2L

# Die Breakpoints selbst werden immer dem letzten der beiden entstehenden
# Intervalle zugerechnet
# Intervall: 1.           2.             3.           4.
# Von/Bis: --> 2015 -- 2015-2017  --  2017-2019 -- 2019 -->
breakpoints <- c("2015-04-01", "2017-01-01", "2019-07-01")

# Spezielle Plot-Optionen für einzelne Intervalle
plotConfiguration <- list(
    # Plot: Anteil Abweichungen > 1/2/5%
    "profitableTriplesByTime" = list(
        # Segmente 1, 2: Legende unter der Grafik anzeigen
        "1" = TRUE,
        "2" = TRUE
    ),
    # Größeren Bereich im Histogramm anzeigen
    "distribution" = list(
        "1" = TRUE
    )
)

#' Tabellen-Template mit `{tableContent}`, `{tableCaption` und `{tableLabel}` 
#' als Platzhalter
summaryTableTemplateFile <- 
    sprintf("%s/Tabellen/Templates/Dreiecksarbitrage_Uebersicht_nach_Boerse.tex",
            latexOutPath)

# Schnellstart für Entwicklung
currency_a <- "usd"
currency_b <- "eur"
threshold <- mainThreshold
onlyMainGraphAndTable <- FALSE
appendThresholdToTableLabel <- FALSE
overviewImageHeight <- NULL
forceTablePosition <- NULL


# Hilfsfunktionen -------------------------------------------------------------

#' Ergebnis berechnen
#' 
#' Berechnet das Ergebnis der Dreiecksarbitrage für beide Forex-Richtungen:
#' Annahme: Bitcoin-Geld-/Briefkurse sind identisch, Devisen-Geld-/Brief nicht.
#' 
#' Daher werden für das Tripel BTC, EUR, USD folgende Routen durchgespielt:
#' Variante 1: EUR - BTC - USD - EUR (EUR-USD-Briefkurs)
#'     äq. zu: USD - EUR - BTC - USD
#'     = `Result_AB`
#' 
#' Variante 2: USD - BTC - EUR - USD (EUR-USD-Geldkurs)
#'     äq. zu: EUR - USD - BTC - EUR
#'     = `Result_BA`
#' 
#' @param result Eine Instanz der Klasse `TriangularResult` (per Referenz)
#' @return NULL (`result` wird per Referenz verändert)
calculateResult <- function(result)
{
    stopifnot(inherits(result, "TriangularResult"))
    
    # Vorliegenden Wechselkurs analysieren und Basis- und quotierte Währung bestimmen
    pair_a_b <- determineCurrencyPairOrder(result$Currency_A, result$Currency_B)
    baseFiatCurrency <- substr(pair_a_b, 1, 3)
    quotedFiatCurrency <- substr(pair_a_b, 4, 6)
    
    if (result$Currency_A == baseFiatCurrency && result$Currency_B == quotedFiatCurrency) {
        #' A ist Basiswährung des Wechselkurses, Beispiel EUR/USD:
        #' A = EUR = Basiswährung
        #' B = USD = quotierte Währung
        #' Umrechnung A -> B (Basis -> quotiert): *Bid (mit Bid multiplizieren)
        #' Umrechnung B -> A (quotiert -> Basis): /Ask (durch Ask dividieren)
        a_to_b <- expr(ab_Bid)
        b_to_a <- expr(1/ab_Ask)
        
    } else if (result$Currency_B == baseFiatCurrency && result$Currency_A == quotedFiatCurrency) {
        #'
        #' * Dies ist der vorliegende Fall für die durchgeführte Berechnung (A=usd, B=eur) *
        #'
        #' B ist Basiswährung des Wechselkurses, Beispiel EUR/USD:
        #' A = USD = quotierte Währung
        #' B = EUR = Basiswährung
        #' Umrechnung A -> B (quotiert -> Basis): /Ask (durch Ask dividieren)
        #' Umrechnung B -> A (Basis -> quotiert): *Bid (mit Bid multiplizieren)
        a_to_b <- expr(1/ab_Ask)
        b_to_a <- expr(ab_Bid)
        
    } else {
        stop("Hinterlegtes Wechselkurspaar ist nicht korrekt!")
    }
    
    # Ergebnisse berechnen (siehe Dokumentation zu data.table: `set`)
    result$data[, `:=`(
        # Route 1 = A->B: A -> B -> BTC -> A oder B -> BTC -> A -> B
        ResultAB = eval(a_to_b) * a_PriceHigh / b_PriceLow - 1,
        
        # Route 2 = B->A: A -> BTC -> B -> A oder B -> A -> BTC -> B
        ResultBA = eval(b_to_a) * b_PriceHigh / a_PriceLow - 1
    )]
    
    #' Bestes Ergebnis bestimmen ("paralleles" Maximum `pmax`)
    result$data[, BestResult := pmax(ResultAB, ResultBA)]
    
    #' Kein Rückgabewert, `result` wird per Referenz verändert
    return(invisible(NULL))
}


#' Aggregiere auf den angegebenen Zeitraum
#' 
#' @param result Eine Instanz der Klasse `TriangularResult` oder eine `data.table`
#' @param floorUnits Aggregations-Zeitfenster, genutzt als `unit` für
#'   `floor_date`. Werte kleiner als ein Tag sind grafisch kaum darstellbar.
#' @param interval Nur dieses Intervall berücksichtigen
#' @return `data.table` mit Min, Q1, Median, Mean, Q3, Max
aggregateResultsByTime <- function(
    result,
    floorUnits,
    interval = NULL
)
{
    # Parameter aufbauen
    if (inherits(result, "TriangularResult")) {
        dataset <- result$data
    } else if (is.data.table(result)) {
        dataset <- result
    }
    
    # Parameter validieren
    stopifnot(
        !is.null(dataset$BestResult),
        is.character(floorUnits), length(floorUnits) == 1L,
        is.null(interval) || length(interval) == 2L
    )
    
    tic()
    
    # Gruppierungsfunktion herausgezogen, um ein Kopieren der
    # großen Datenmengen (für den Fall, dass ein Intervall angegeben ist)
    # unten zu vermeiden (stattdessen immer direkt ein data.table-Subsetting)
    group <- expr(.(
        
        # Lagemaße
        Min = min(BestResult),
        Q1 = quantile(BestResult, probs=.25, names=FALSE),
        Mean = mean(BestResult),
        Median = median(BestResult),
        Q3 = quantile(BestResult, probs=.75, names=FALSE),
        Max = max(BestResult),
        n = .N,
        
        # Wieviele Arbitrageergebnisse sind größer als...
        # 1 %
        nLargerThan1Pct = length(which(BestResult >= .01)),
        # 2 %
        nLargerThan2Pct = length(which(BestResult >= .02)),
        # 5 %
        nLargerThan5Pct = length(which(BestResult >= .05)),
        
        # Routen-Statistiken: Wie oft ist Route 1 / 2 die bessere Wahl?
        AB_Best = length(which(ResultAB > ResultBA)) / .N,
        BA_Best = length(which(ResultBA > ResultAB)) / .N
    ))
    
    if (!is.null(interval)) {
        
        # Zeitraum begrenzt
        aggregatedResults <- dataset[
            Time %between% interval,
            j = eval(group),
            by = .(Time=floor_date(Time, unit=floorUnits))
        ]
        
    } else {
        
        # Gesamte Daten
        aggregatedResults <- dataset[
            j = eval(group),
            by = .(Time=floor_date(Time, unit=floorUnits))
        ]
    }
    
    setorder(aggregatedResults, Time)
    printf("Aggregation auf '%s' ergab %s Datensätze. ",
           floorUnits, format.number(nrow(aggregatedResults)))
    toc()
    
    return(aggregatedResults)
}


#' Zeichne Arbitrageergebnisse als Linien-/Punktgrafik im Zeitverlauf
#' 
#' @param arbitrageResults `data.table` mit den aggregierten Ergebnissen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param plotTitle Überschrift (optional)
#' @return Der Plot (unsichtbar)
plotAggregatedResultsOverTime <- function(
    arbitrageResults,
    latexOutPath = NULL,
    breakpoints = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(arbitrageResults), nrow(arbitrageResults) > 0L,
        !is.null(arbitrageResults$Time), !is.null(arbitrageResults$Q1),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L)
    )
    
    # Einige Bezeichnungen und Variablen
    plotTextPrefix <- "\\footnotesize "
    plotSmallPrefix <- "\\small "
    plotXLab <- "Datum"
    plotYLab <- "Arbitrageergebnis"
    
    # Zeichnen
    minValue <- min(arbitrageResults$Q1)
    maxValue <- max(arbitrageResults$Q3)
    
    plot <- ggplot(arbitrageResults)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(arbitrageResults$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(xmin=From, xmax=To, ymin=minValue, ymax=maxValue * 1.05, fill=Set),
                data = intervals,
                alpha = .5,
                show.legend = FALSE
            ) +
            geom_text(
                aes(x=From+(To-From)/2, y=maxValue, label=paste0(plotTextPrefix, Set)),
                data = intervals
            )
    }
    
    # Datumsachse bestimmen
    if (difftime(max(arbitrageResults$Time), min(arbitrageResults$Time), units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    plot <- plot +
        # Q1/Q3 zeichnen
        geom_ribbon(aes(x=Time, ymin=Q1, ymax=Q3), fill="grey70") +
        
        # Median zeichnen
        geom_line(aes(x=Time, y=Median, color="1", linetype="1"), size=.5, show.legend=FALSE) +
        
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        coord_cartesian(ylim=c(minValue, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(
            labels = function(x) paste(format.number(x * 100), "%")
        ) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0(plotSmallPrefix, plotTitle))
    }
    
    # Ausgabe als LaTeX-Dokument
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        print(plot)
        dev.off()
        return(invisible(plot))
    }
    
    # Sonst nur Plot zurückgeben
    return(plot)
}


#' Anteil der "vorteilhaften" Preisabweichungen im Zeitverlauf darstellen
#' 
#' @param result `data.table` mit den aggr. Preistripeln
#' @param latexOutPath Zieldatei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param plotTitle Überschrift (optional)
#' @param legendBelowGraph Position der Legende (optional)
#' @return Der Plot (unsichtbar)
plotProfitableTriplesByTime <- function(
    result,
    latexOutPath = NULL,
    breakpoints = NULL,
    plotTitle = NULL,
    legendBelowGraph = NULL
)
{
    # Parameter validieren
    stopifnot(
        is.data.table(result),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (is.character(plotTitle) && length(plotTitle) == 1L),
        is.null(legendBelowGraph) || (is.logical(legendBelowGraph) && length(legendBelowGraph) == 1L)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- "Anteil"
    legendName <- "Grenzwert"
    plotTextPrefix <- "\\footnotesize "
    plotSmallPrefix <- "\\small "
    
    # Gesamtübersicht, gruppiert nach Grenzwerten, aggregiert
    plot <- ggplot(result)
    
    # Maximalwert entsteht immer bei der geringsten Abweichung (Hier: 1%)
    maxValue <- with(result, max(nLargerThan1Pct / n))
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(result$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(xmin=From, xmax=To, ymin=0, ymax=maxValue * 1.05, fill=Set),
                data = intervals,
                alpha = .5,
                show.legend = FALSE
            ) +
            geom_text(
                aes(x=From+(To-From)/2, y=maxValue, label=paste0(plotTextPrefix, Set)),
                data = intervals
            )
    }
    
    # Datumsachse bestimmen
    if (difftime(max(result$Time), min(result$Time), units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    # Legende einrichten
    if (isTRUE(legendBelowGraph)) {
        legendPosition <- "bottom"
        legendMargin <- margin(t=-5)
        legendBackground <- element_blank()
    } else if (is.null(breakpoints)) {
        legendPosition <- c(0.88, 0.7)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    } else {
        legendPosition <- c(0.88, 0.5)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    }
    
    # Grafik mit Anteilen zeichnen
    plot <- plot +
        geom_line(aes(x=Time, y=nLargerThan1Pct/n, color="1\\,%", linetype="1\\,%"), size = .5) +
        geom_line(aes(x=Time, y=nLargerThan2Pct/n, color="2\\,%", linetype="2\\,%"), size = .5) +
        geom_line(aes(x=Time, y=nLargerThan5Pct/n, color="5\\,%", linetype="5\\,%"), size = .5) +
        
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5)),
            axis.title.y = element_text(margin=margin(r=10)),
            legend.position = legendPosition,
            legend.background = legendBackground,
            legend.margin = legendMargin,
            legend.title = element_blank()
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(
            labels = function(x) paste0(format.percentage(x, digits=0L), "\\,%")
        ) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab),
            linetype = paste0(plotTextPrefix, legendName),
            colour = paste0(plotTextPrefix, legendName)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0(plotSmallPrefix, plotTitle))
    }
    
    # Ausgabe als LaTeX-Dokument
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        print(plot)
        dev.off()
        return(invisible(plot))
    }
    
    # Sonst nur Plot zurückgeben
    return(plot)
}


#' Zeichne Anzahl der Beobachtungen als Liniengrafik im Zeitverlauf
#' 
#' @param arbitrageResults `data.table` mit den aggregierten Ergebnissen
#' @return Der Plot (unsichtbar)
plotNumResultsOverTime <- function(
    arbitrageResults,
    breakpoints = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(arbitrageResults), nrow(arbitrageResults) > 0L,
        !is.null(arbitrageResults$Time)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- "Beobachtungen"
    plotTextPrefix <- "\\footnotesize "
    plotSmallPrefix <- "\\small "
    maxValue <- max(arbitrageResults$n)
    
    # Achseneigenschaften
    if (maxValue > 1e6) {
        roundedTo <- " [Mio.]"
        roundFac <- 1e6
    } else if (maxValue > 1e3) {
        roundedTo <- " [Tsd.]"
        roundFac <- 1e3
    } else {
        roundedTo <- ""
        roundFac <- 1
    }
    
    plot <- ggplot(arbitrageResults)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(arbitrageResults$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(xmin=From, xmax=To, ymin=0, ymax=maxValue * 1.05, fill=Set),
                data = intervals,
                alpha = .5,
                show.legend = FALSE
            ) +
            geom_text(
                aes(x=From+(To-From)/2, y=maxValue, label=paste0(plotTextPrefix, Set)),
                data = intervals
            )
    }
    
    # Datumsachse bestimmen
    if (difftime(max(arbitrageResults$Time), min(arbitrageResults$Time), units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    # Anzahl Datensätze zeichnen
    plot <- plot +
        geom_line(aes(x=Time, y=n, color="1", linetype="1"), size=.5) +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            legend.position = "none"
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(
            labels = function(x) paste(format.number(x / roundFac))
        ) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, roundedTo)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0(plotSmallPrefix, plotTitle))
    }
    
    return(plot)
}


#' Volatilität (Mittelwert) zweier Kurspaare einer Börse für den
#' angegebenen Zeitabschnitt darstellen
#' 
#' @param pair1 Das gewünschte Kurspaar, bspw. btcusd
#' @param pair2 Das gewünschte Kurspaar, bspw. btceur
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @param exchange Das gewünschte Kurspaar, bspw. btcusd
#' @param numPeriodsForEstimate Anzahl Perioden für die Vola.-Abschätzung
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param plotTitle Überschrift (optional)
#' @param legendBelowGraph Position der Legende (optional)
#' @return Plot
plotVolatilityByTime <- function(
    pair1,
    pair2,
    timeframe,
    exchange = NULL,
    numPeriodsForEstimate = 10L,
    breakpoints = NULL,
    plotTitle = NULL,
    legendBelowGraph = NULL
)
{
    stopifnot(
        is.character(pair1), length(pair1) == 1L,
        is.character(pair2), length(pair2) == 1L,
        length(timeframe) == 2L, is.POSIXct(timeframe),
        is.null(exchange) || (length(exchange) == 1L && is.character(exchange)),
        is.numeric(numPeriodsForEstimate), length(numPeriodsForEstimate) == 1L,
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle)),
        is.null(legendBelowGraph) || (is.logical(legendBelowGraph) && length(legendBelowGraph) == 1L)
    )
    
    # Zeitzone ist UTC, fehlt aber in `timeframe` aus unbekannten Gründen
    setattr(timeframe, "tzone", "UTC")
    
    plotXLab <- "Datum"
    plotYLab <- "Volatilität"
    plotTextPrefix <- "\\footnotesize "
    
    # Volatilität berechnen
    
    if (is.null(exchange)) {
        exchanges <- names(exchangeNames)
    } else {
        exchanges <- c(exchange)
    }
    result <- data.table()
    
    for (exchangeLoop in exchanges) {
        
        # Paar 1
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-daily.fst",
            tolower(exchangeLoop), tolower(pair1)
        )
        stopifnot(file.exists(sourceFile))
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Mean"), as.data.table=TRUE)
        dataset[, Pair:=format.currencyPair(pair1)]
        dataset[, Volatility:=volatility(dataset$Mean, n=numPeriodsForEstimate, N=365)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe & !is.na(Volatility)]))
        
        # Paar 2
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-daily.fst",
            tolower(exchangeLoop), tolower(pair2)
        )
        stopifnot(file.exists(sourceFile))
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Mean"), as.data.table=TRUE)
        dataset[, Pair:=format.currencyPair(pair2)]
        dataset[, Volatility:=volatility(dataset$Mean, n=numPeriodsForEstimate, N=365)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe & !is.na(Volatility)]))
        rm(dataset)
    }
    
    stopifnot(nrow(result) > 0L)
    
    # Gruppieren und sortieren
    result <- result[j=.(Volatility=mean(Volatility)), by=.(Time, Pair)]
    setorder(result, Time, Pair)
    
    # Achseneigenschaften
    maxValue <- max(result$Volatility)
    
    plot <- ggplot(result)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(result$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(xmin=From, xmax=To, ymin=0, ymax=maxValue * 1.05, fill=Set),
                data = intervals,
                alpha = .5,
                show.legend = FALSE
            ) +
            geom_text(
                aes(x=From+(To-From)/2, y=maxValue, label=paste0(plotTextPrefix, Set)),
                data = intervals
            )
    }
    
    # Datumsachse bestimmen
    if (difftime(timeframe[2], timeframe[1], units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    # Legende einrichten
    if (isTRUE(legendBelowGraph)) {
        legendPosition <- "bottom"
        legendMargin <- margin(t=-5)
        legendBackground <- element_blank()
    } else if (is.null(exchange)) {
        legendPosition <- "right"
        legendMargin <- margin()
        legendBackground <- element_blank()
    } else if (is.null(breakpoints)) {
        legendPosition <- c(0.88, 0.7)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    } else {
        legendPosition <- c(0.88, 0.66)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    }
    
    # Volumen zeichnen
    plot <- plot +
        geom_line(aes(x=Time, y=Volatility, color=Pair, linetype=Pair), size=.5) +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            legend.position = legendPosition,
            legend.background = legendBackground,
            legend.margin = legendMargin,
            legend.title = element_blank()
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(labels = format.number) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab),
            color = paste0(plotTextPrefix, "Kurspaar"),
            linetype = paste0(plotTextPrefix, "Kurspaar")
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Zeichne das gehandelte Volumen eines Kurspaares an allen Börsen für den
#' angegebenen Zeitabschnitt.
#' 
#' @param pair1 Das gewünschte Kurspaar, bspw. btcusd
#' @param pair2 Das gewünschte Kurspaar, bspw. btceur
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @param legendBelowGraph Position der Legende (optional)
#' @return Plot
plotTradingVolumeByTime <- function(
    pair1,
    pair2,
    timeframe,
    breakpoints = NULL,
    plotTitle = NULL,
    aggregationLevel = "monthly",
    legendBelowGraph = NULL
)
{
    stopifnot(
        is.character(pair1), length(pair1) == 1L,
        is.character(pair2), length(pair2) == 1L,
        length(timeframe) == 2L, is.POSIXct(timeframe),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle)),
        length(aggregationLevel) == 1L, is.character(aggregationLevel)
    )
    
    # Zeitzone ist UTC, fehlt aber in `timeframe` aus unbekannten Gründen
    setattr(timeframe, "tzone", "UTC")
    
    plotXLab = "Datum"
    plotYLab = "Volumen"
    plotTextPrefix <- "\\footnotesize "
    
    # Datenquelle auswählen
    if (aggregationLevel == "monthly") {
        aggregationSourceLevel <- "monthly"
    } else if (
        aggregationLevel == "weekly" ||
        aggregationLevel == "daily"
    ) {
        aggregationSourceLevel <- "daily"
    } else {
        stop(sprintf("Ungültiges Aggregationslevel: %s",))
    }
    
    # Handelsvolumen berechnen
    result <- data.table()
    for (i in seq_along(exchangeNames)) {
        exchange <- names(exchangeNames)[[i]]
        
        # Paar 1
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-%3$s.fst",
            tolower(exchange), tolower(pair1), aggregationSourceLevel
        )
        stopifnot(file.exists(sourceFile))
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Amount"), as.data.table=TRUE)
        dataset[, Pair:=format.currencyPair(pair1)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
        
        # Paar 2
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-%3$s.fst",
            tolower(exchange), tolower(pair2), aggregationSourceLevel
        )
        stopifnot(file.exists(sourceFile))
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Amount"), as.data.table=TRUE)
        dataset[, Pair:=format.currencyPair(pair2)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
    }
    
    stopifnot(nrow(result) > 0L)
    
    if (aggregationLevel == "weekly") {
        # Auf Wochendaten aggregieren
        result <- result[j=.(Amount=sum(Amount)), by=.(Time=floor_date(Time, "1 week"), Pair)]
    } else {
        # Direkt zusammenfassen
        result <- result[j=.(Amount=sum(Amount)), by=.(Time, Pair)]
    }
    setorder(result, Time)
    
    # Achseneigenschaften
    maxValue <- max(result$Amount)
    if (maxValue > 1e6) {
        roundedTo <- " [Mio.]"
        roundFac <- 1e6
    } else if (maxValue > 1e3) {
        roundedTo <- " [Tsd.]"
        roundFac <- 1e3
    } else {
        roundedTo <- ""
        roundFac <- 1
    }
    
    # Legende einrichten
    if (isTRUE(legendBelowGraph)) {
        legendPosition <- "bottom"
        legendMargin <- margin(t=-5)
        legendBackground <- element_blank()
    } else {
        legendPosition <- c(0.1, 0.58)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    }
    
    plot <- ggplot(result)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(result$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(xmin=From, xmax=To, ymin=0, ymax=maxValue * 1.05, fill=Set),
                data = intervals,
                alpha = .5,
                show.legend = FALSE
            ) +
            geom_text(
                aes(x=From+(To-From)/2, y=maxValue, label=paste0(plotTextPrefix, Set)),
                data = intervals
            )
    }
    
    # Datumsachse bestimmen
    if (difftime(timeframe[2], timeframe[1], units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    # Volumen zeichnen
    plot <- plot +
        geom_line(aes(x=Time, y=Amount, color=Pair, linetype=Pair), size=.5) +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            legend.title = element_blank(),
            legend.position = legendPosition,
            legend.background = legendBackground,
            legend.margin = legendMargin
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(labels = function(x) format.number(x/roundFac)) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, roundedTo),
            color = paste0(plotTextPrefix, "Kurspaar"),
            linetype = paste0(plotTextPrefix, "Kurspaar")
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Preisniveau (Mittelwert) eines Kurspaares an allen Börsen für den
#' angegebenen Zeitabschnitt
#' 
#' @param pair1 Das gewünschte Kurspaar, bspw. btcusd
#' @param pair2 Das gewünschte Kurspaar, bspw. btceur
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @return Plot
plotPriceLevelByTime <- function(
    pair1,
    pair2,
    timeframe,
    breakpoints = NULL,
    plotTitle = NULL,
    aggregationLevel = "weekly"
)
{
    stopifnot(
        is.character(pair1), length(pair1) == 1L,
        is.character(pair2), length(pair2) == 1L,
        length(timeframe) == 2L, is.POSIXct(timeframe),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle)),
        length(aggregationLevel) == 1L, is.character(aggregationLevel)
    )
    
    # Zeitzone ist UTC, fehlt aber in `timeframe` aus unbekannten Gründen
    setattr(timeframe, "tzone", "UTC")
    
    plotXLab <- "Datum"
    plotYLab <- "Preis"
    plotTextPrefix <- "\\footnotesize "
    
    # Datenquelle auswählen
    if (aggregationLevel == "monthly") {
        aggregationSourceLevel <- "monthly"
    } else if (
        aggregationLevel == "weekly" ||
        aggregationLevel == "daily"
    ) {
        aggregationSourceLevel <- "daily"
    } else {
        stop(sprintf("Ungültiges Aggregationslevel: %s",))
    }
    
    # Preisniveau
    result <- data.table()
    for (i in seq_along(exchangeNames)) {
        exchange <- names(exchangeNames)[[i]]
        
        # Paar 1
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-%3$s.fst",
            tolower(exchange), tolower(pair1), aggregationSourceLevel
        )
        stopifnot(file.exists(sourceFile))
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Mean"), as.data.table=TRUE)
        dataset[, Pair:=format.currencyPair(pair1)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
        
        # Paar 2
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-%3$s.fst",
            tolower(exchange), tolower(pair2), aggregationSourceLevel
        )
        stopifnot(file.exists(sourceFile))
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Mean"), as.data.table=TRUE)
        dataset[, Pair:=format.currencyPair(pair2)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
    }
    stopifnot(nrow(result) > 0L)
    
    if (aggregationLevel == "weekly") {
        # Auf Wochendaten aggregieren
        result <- result[j=.(Mean=mean(Mean)), by=.(Time=floor_date(Time, "1 week"), Pair)]
    } else {
        # Direkt zusammenfassen
        result <- result[j=.(Mean=mean(Mean)), by=.(Time, Pair)]
    }
    setorder(result, Time)
    
    # Achseneigenschaften
    maxValue <- max(result$Mean)
    if (maxValue > 1e6) {
        roundedTo <- sprintf(" [Mio.]",)
        roundFac <- 1e6
    } else if (maxValue > 1e3) {
        roundedTo <- sprintf(" [Tsd.]")
        roundFac <- 1e3
    } else {
        roundedTo <- ""
        roundFac <- 1
    }
    
    plot <- ggplot(result)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(result$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(xmin=From, xmax=To, ymin=0, ymax=maxValue * 1.05, fill=Set),
                data = intervals,
                alpha = .5,
                show.legend = FALSE
            ) +
            geom_text(
                aes(x=From+(To-From)/2, y=maxValue, label=paste0(plotTextPrefix, Set)),
                data = intervals
            )
    }
    
    # Datumsachse bestimmen
    if (difftime(timeframe[2], timeframe[1], units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    # Volumen zeichnen
    plot <- plot +
        geom_line(aes(x=Time, y=Mean, color=Pair, linetype=Pair), size=.5) +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            legend.title = element_blank(),
            legend.position = c(0.13, 0.55),
            legend.margin = margin(0, 12, 5, 5),
            legend.background = element_rect(fill="white", size=0.2, linetype="solid")
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(labels = function(x) format.number(x/roundFac)) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, roundedTo),
            color = paste0(plotTextPrefix, "Kurspaar"),
            linetype = paste0(plotTextPrefix, "Kurspaar")
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Zeichne Arbitrageergebnis als Boxplot nach Börse gruppiert
#' 
#' @param comparablePrices `data.table` mit den Ergebnissen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @return Der Plot (unsichtbar)
plotResultBoxplotByExchange <- function(
    result,
    latexOutPath = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(result), nrow(result) > 0L, !is.null(result$BestResult),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))
    )
    
    # Achsenbeschriftung
    plotXLab <- "Börse"
    plotYLab <- "Arbitrageergebnis"
    plotTextPrefix <- "\\footnotesize "
    
    # Zeichnen
    minValue <- min(result$BestResult)
    maxValue <- max(result$BestResult)
    
    # Werte berechnen: Wesentlich schneller,
    # als geom_boxplot die Berechnung übernehmen zu lassen.
    # Das grafische Ergebnis ist identisch, da ohnehin 
    # keine Ausreißer angezeigt werden.
    boxplot_stats <- data.table()
    for (i in seq_along(exchangeNames)) {
        exchangeName <- exchangeNames[[i]]
        
        # Diese Börse ist in der aktuellen Auswahl nicht enthalten
        if (result[Exchange == exchangeName, .N] == 0L) {
            next
        }
        
        exchangeSummary <- summary(result[Exchange == exchangeName, BestResult])
        
        iqr <- exchangeSummary[[5]] - exchangeSummary[[2]]
        boxplot_stats <- rbindlist(list(boxplot_stats, data.table(
            Exchange = exchangeName,
            ColorGroup = "Börse",
            min = max(exchangeSummary[[1]], exchangeSummary[[2]] - 1.5*iqr),
            lower = exchangeSummary[[2]],
            middle = exchangeSummary[[3]],
            upper = exchangeSummary[[5]],
            max = min(exchangeSummary[[6]], exchangeSummary[[5]] + 1.5*iqr)
        )))
    }
    
    # Globale Statistiken nur dann, wenn mehr als eine Börse vorhanden ist
    if (nrow(boxplot_stats) > 1L) {
        totalSummary <- summary(result$BestResult)
        iqr <- totalSummary[[5]] - totalSummary[[2]]
        boxplot_stats <- rbindlist(list(boxplot_stats, data.table(
            Exchange = "Gesamt",
            ColorGroup = "Gesamt",
            min = max(totalSummary[[1]], totalSummary[[2]] - 1.5*iqr),
            lower = totalSummary[[2]],
            middle = totalSummary[[3]],
            upper = totalSummary[[5]],
            max = min(totalSummary[[6]], totalSummary[[5]] + 1.5*iqr)
        )))
        
        # Sortierreihenfolge setzen
        boxplot_stats[,Exchange:=factor(Exchange, levels=c("Gesamt", unlist(unname(exchangeNames))))]
    }
    
    # Bedeutung der Werte:
    # ymin = "...", # Kleinster Wert, der nicht Ausreißer ist = (lower - 1.5*IQR)
    # lower = "...", # Untere Grenze der Box = 25%-Quartil
    # middle = "...", # Median
    # upper = "...", # Obere Grenze der Box = 75%-Quartil
    # ymax = "...", # Größter Wert, der nicht Ausreißer ist (= upper + 1.5*IQR)
    
    # Boxplot, gruppiert nach Börse
    plot <- ggplot(boxplot_stats) +
        
        # Horizontale Linien an den Whiskern
        geom_errorbar(
            aes(x=Exchange, ymin=min, ymax=max, group=Exchange, color=ColorGroup),
            width = .75,
            position = position_dodge(width = 0.9)
        ) +
        
        # Boxplot
        geom_boxplot(
            aes(
                x = Exchange,
                ymin = min, lower = lower, middle = middle, upper = upper, ymax = max,
                group = Exchange,
                color = ColorGroup
            ),
            stat = "identity",
            width = .75
        ) +
        scale_y_continuous(
            labels = function(x) paste0(format.number(x * 100), "\\,%"),
            limits = c(min(boxplot_stats$min), max(boxplot_stats$max)) * 1.0005
        ) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0)),
            plot.margin = margin(7.5, 5.5, 5.5, 5.5) # vgl. theme_get()$plot.margin
        ) +
        scale_color_ptol() +
        scale_fill_ptol() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    # Ausgabe als LaTeX-Dokument
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        print(plot)
        dev.off()
        return(invisible(plot))
    }
    
    # Sonst nur Plot zurückgeben
    return(plot)
}


#' Zeichne Verteilung der Beobachtungen als Histogramm
#' 
#' @param result `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param plotTitle Optionaler Plot-Titel
#' @param largeThresholds Größeren Bereich anzeigen (nur bestimmte Intervalle)
#' @return Der Plot (unsichtbar)
plotDistribution <- function(
    result,
    latexOutPath = NULL,
    plotTitle = NULL,
    largeThresholds = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(result), nrow(result) > 0L,
        !is.null(result$BestResult),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle)),
        is.null(largeThresholds) || (length(largeThresholds) == 1L && is.logical(largeThresholds))
    )
    
    # Berechnung der Verteilung
    if (is.null(largeThresholds)) {
        floorToNumberOfDigits <- 4L
        roundBy <- 1L
        cutoffThresholds <- c(-0.0005, 0.02)
        breaks <- expr(seq(from=-0.02, to=0.02, by=0.002))
        barWidth <- 0.75 # Standardwert: 90% = 0.9
    } else {
        floorToNumberOfDigits <- 3L
        roundBy <- 2L
        cutoffThresholds <- c(-0.02, 0.05)
        breaks <- expr(waiver())
        barWidth <- 2
    }
    
    distribution <- result[
        # Nur Preisunterschiede unterhalb des Grenzwertes anzeigen
        i = BestResult >= cutoffThresholds[1] & BestResult <= cutoffThresholds[2],
        # Häufigkeit zählen
        j = .(n = .N),
        # Gruppieren nach dem gerundeten Wert
        by = .(
            BestResult = floor(
                BestResult * 10^floorToNumberOfDigits * roundBy
            ) / 10^floorToNumberOfDigits / roundBy
        )
    ]
    
    # Sortierung nur optional für schönere Darstellung im Terminal etc.
    #setorder(distribution, BestResult)
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Arbitrageergebnis"
    plotYLab <- "Beobachtungen"
    plotTextPrefix <- "\\footnotesize "
    maxValue <- max(distribution$n)
    
    # Achseneigenschaften
    if (maxValue > 1e6) {
        roundedTo <- " [Mio.]"
        roundFac <- 1e6
    } else if (maxValue > 1e3) {
        roundedTo <- " [Tsd.]"
        roundFac <- 1e3
    } else {
        roundedTo <- ""
        roundFac <- 1L
    }
    
    # Histogramm zeichnen
    plot <- ggplot(distribution) +
        geom_col(
            aes(x = BestResult, y = n, fill = "Häufigkeit"),
            width = 1 / nrow(distribution) / 100 * barWidth,
            show.legend = FALSE
        ) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        scale_x_continuous(
            labels = function(x) { paste0(format.percentage(x, 1L), "\\,\\%") },
            breaks = eval(breaks),
            expand = expansion(mult=c(.01, .03))
        ) +
        scale_y_continuous(labels=function(x) format.number(x / roundFac)) +
        scale_color_ptol() +
        scale_fill_ptol() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, roundedTo)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    # Ausgabe als LaTeX-Dokument
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 5 / 2.54, # cm -> Zoll
            sanitize = FALSE
        )
        print(plot)
        dev.off()
        return(invisible(plot))
    }
    
    # Sonst nur Plot zurückgeben
    return(plot)
}


#' Zeichne Anteil vorteilhafter Routen nach Börse
#' 
#' @param aggregatedResult `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param plotTitle Optionaler Plot-Titel
#' @return Der Plot (unsichtbar)
plotPercentageBestByRoute <- function(
    allResults,
    latexOutPath = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(allResults), nrow(allResults) > 0L,
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))
    )
    
    # Ergebnis berechnen
    result <- data.table()
    
    # 1. Anzahl gesamter Tauschpaare zählen
    for (i in seq_along(exchangeNames)) {
        exchange <- names(exchangeNames)[[i]]
        exchangeName <- exchangeNames[[i]]
        result <- rbindlist(list(result, list(
            exchange = exchange,
            exchangeName = exchangeName,
            n = allResults[Exchange == exchangeName, .N]
        )))
    }
    
    result <- allResults[
        j = .(
            # Routen-Statistiken: Wie oft ist Route 1 / 2 die bessere Wahl?
            AB_Best = length(which(ResultAB > ResultBA)) / .N,
            BA_Best = length(which(ResultBA > ResultAB)) / .N
        ),
        by = Exchange
    ]
    
    # Melt für ggplot
    result <- melt(result, id.vars=c("Exchange"), variable.name="type", value.name="ratio")
    setorder(result, Exchange)
    result[, type:=factor(
        type,
        levels = c("AB_Best", "BA_Best"),
        labels = c("Route 1", "Route 2")
    )]
    
    # Plot
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Börse"
    plotYLab <- "Anteil"
    plotFillLab <- "Route"
    plotTextPrefix <- "\\footnotesize "
    
    # Histogramm zeichnen
    plot <- ggplot(result) +
        geom_col(aes(x=Exchange, y=ratio, fill=type), position="dodge", width=.75) +
        geom_text(
            aes(
                x = Exchange,
                y = ratio + .07,
                group = type,
                label = paste0("\\scriptsize ", format.percentage(ratio, 1L), "\\,\\%")
            ),
            position = position_dodge(width=.75)
        ) +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            legend.title = element_blank()
        ) +
        scale_y_continuous(labels=function(x) paste0(format.percentage(x, 0L), "\\,\\%")) +
        scale_color_ptol() +
        scale_fill_ptol() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab),
            fill = plotFillLab
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    # Ausgabe als LaTeX-Dokument
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 5 / 2.54, # cm -> Zoll
            sanitize = FALSE
        )
        print(plot)
        dev.off()
        return(invisible(plot))
    }
    
    # Sonst nur Plot zurückgeben
    return(plot)
}


#' Informationen über einen Datensatz als LaTeX-Tabelle ausgeben
#' 
#' @param dataset `data.table` aus `loadComparablePricesByCurrency`
#'                             oder aus `aggregatePriceDifferences`
#' @param outFile Zieldatei
#' @param caption Tabellentitel
#' @param indexCaption Tabellentitel für Tabellenverzeichnis
#' @param forceTablePosition Bestimmte Tabellenposition erzwingen
#' @param label Tabellenlabel
summariseDatasetAsTable <- function(
    dataset,
    outFile = NULL,
    caption = NULL,
    indexCaption = NULL,
    label = NULL,
    forceTablePosition = NULL
)
{
    # Parameter validieren
    stopifnot(
        is.data.table(dataset), nrow(dataset) > 0L,
        is.null(outFile) || (length(outFile) == 1L && is.character(outFile)),
        is.null(caption) || (length(caption) == 1L && is.character(caption)),
        is.null(indexCaption) || (length(indexCaption) == 1L && is.character(indexCaption)),
        is.null(label) || (length(label) == 1L && is.character(label)),
        is.null(forceTablePosition) ||
            (length(forceTablePosition) == 1L && is.character(forceTablePosition))
    )
    if (is.null(indexCaption) && !is.null(caption)) {
        indexCaption <- caption
    }
    
    if (!is.null(outFile)) {
        printf("Erzeuge Überblickstabelle in %s... ", basename(outFile))
    }
    numRowsTotal <- nrow(dataset)
    
    # Tabellenzeile erzeugen
    createRow <- function(numRows, dataSubset, lineEnd="\\\\\n\n") {
        
        # Fortschrittsanzeige
        printf(".")
        
        # Intervalldauer berechnen
        intervalLengthHours <- 
            difftime(last(dataSubset$Time), first(dataSubset$Time), units = "hours") |>
            round() |>
            as.double()
        
        # Anzahl Paare pro Stunde bzw. pro Tag
        numRowsPerHour <- numRows / intervalLengthHours
        numRowsPerDay <- numRows / (intervalLengthHours / 24)
        
        # Anzahl "vorteilhafter" Arbitrage-Tripel bestimmen
        # > 1 %
        numRowsLargerThan_A <- length(which(dataSubset$BestResult >= .01))
        # > 2 %
        numRowsLargerThan_B <- length(which(dataSubset$BestResult >= .02))
        # > 5 %
        numRowsLargerThan_C <- length(which(dataSubset$BestResult >= .05))
        
        # Einrückung in der Ergebnisdatei
        s <- strrep(" ", 12)
        
        # Zeile erzeugen
        return(paste0(
            
            # Anzahl Datensätze pro Tag (nur Kommentar)
            s, sprintf(
                "%% %s Datensätze pro Tag\n",
                format.numberWithFixedDigits(numRowsPerDay, digits=1L)
            ),
            
            # Anzahl Datensätze (absolut/relativ)
            s, sprintf(
                "\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n",
                format.number(numRows),
                format.percentage(numRows / numRowsTotal, 1L)
            ),
            
            # Anzahl Datensätze pro Stunde
            s, sprintf("%s &\n", format.numberWithFixedDigits(numRowsPerHour, 1)),
            
            # Anzahl/Anteil Datensätze > 1%
            s, sprintf(
                "\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n",
                format.number(numRowsLargerThan_A),
                format.percentage(numRowsLargerThan_A / numRows, 1L)
            ),
            
            # Anzahl/Anteil Datensätze > 2%
            s, sprintf(
                "\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n",
                format.number(numRowsLargerThan_B),
                format.percentage(numRowsLargerThan_B / numRows, 1L)
            ),
            
            # Anzahl/Anteil Datensätze > 5%
            s, sprintf(
                "\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n",
                format.number(numRowsLargerThan_C),
                format.percentage(numRowsLargerThan_C / numRows, 1L)
            ),
            
            # Median
            s, sprintf(
                "%s\\,\\%% ",
                format.percentage(median(dataSubset$BestResult), 1)
            ),
            
            # Zeilenende
            lineEnd
        ))
    }
    
    # Tabelleninhalt initialisieren
    tableContent <- ""
    
    # Tabelle kann im Fließtext unterkommen
    tablePosition <- "tbh"
    
    # Börsen in diesem Datensatz
    exchangesInThisSubset <- unique(dataset$Exchange)
    
    if (!is.null(forceTablePosition)) {
        tablePosition <- forceTablePosition
    } else {
        # Tabelle kann im Fließtext unterkommen
        tablePosition <- "tbh"
    }
    
    if (length(exchangesInThisSubset) == 1L) {
        
        # Nur eine Börse: Kompakte Statistiken für gesamten Datensatz
        exchange <- exchangesInThisSubset[1L]
        tableContent <- paste0(
            tableContent,
            sprintf("        %s &\n", exchange),
            createRow(numRowsTotal, dataset)
        )
        
    } else {
        
        # Jede Börse durchgehen + Gesamtstatistik erstellen
        for (i in seq_along(exchangeNames)) {
            exchange <- exchangeNames[[i]]
            allRows <- dataset[Exchange == exchange, which=TRUE]
            if (length(allRows) > 0L) {
                
                # Gesamtüberblick für diese Börse
                tableContent <- paste0(
                    tableContent, "\n",
                    sprintf("        %s &\n", exchange),
                    createRow(length(allRows), dataset[allRows])
                )
            }
        }
        
        # Gesamtdaten
        tableContent <- paste0(
            tableContent,
            "        \\tablebody\n\n",
            "        Gesamt &\n",
            createRow(nrow(dataset), dataset)
        )
    }
    
    printf("\n")
    
    # Tabelle einfach ausgeben
    if (is.null(outFile)) {
        cat(tableContent)
        return(invisible(NULL))
    }
    
    # Tabelle schreiben
    if (file.exists(outFile)) {
        Sys.chmod(outFile, mode="0644")
    }
    summaryTableTemplateFile |>
        read_file() |>
        str_replace(coll("{tablePosition}"), tablePosition) |>
        str_replace(coll("{tableCaption}"), caption) |>
        str_replace(coll("{tableIndexCaption}"), indexCaption) |>
        str_replace(coll("{tableContent}"), tableContent) |>
        str_replace(coll("{tableLabel}"), label) |>
        write_file(outFile)
    
    # Vor versehentlichem Überschreiben schützen
    Sys.chmod(outFile, mode="0444")
    
    return(invisible(NULL))
}



# Haupt-Auswertungsfunktion ---------------------------------------------------

#' Berechne und analysiere Ergebnisse der Dreiecksarbitrage in grafischer
#' Form und berechne Daten für eine Auswertung in tabellarischer Form
#'
#' @param currency_a Gegenwährung 1
#' @param currency_b Gegenwährung 2
#' @param threshold Zeitliche Differenz zweier BTC-Ticks in Sekunden,
#'                  ab der das Tick-Paar verworfen wird.
#' @param onlyMainGraphAndTable Grafiken und Tabellen für Teil-Intervalle (nicht) erstellen
#' @param appendThresholdToTableLabel Grenzwert an Tabellen-Label anhängen
#' @param overviewImageHeight Höhe der Überblicksgrafik überschreiben
#' @return `data.table`: Ergebnis von `collectDatasetSummary()`
analyseTriangularArbitrage <- function(
    currency_a,
    currency_b,
    threshold,
    onlyMainGraphAndTable = FALSE,
    appendThresholdToTableLabel = FALSE,
    overviewImageHeight = NULL
)
{
    # Parameter validieren
    stopifnot(
        is.character(currency_a), length(currency_a) == 1L, nchar(currency_a) == 3L,
        is.character(currency_b), length(currency_b) == 1L, nchar(currency_b) == 3L,
        is.numeric(threshold), length(threshold) == 1L,
        is.logical(onlyMainGraphAndTable), length(onlyMainGraphAndTable) == 1L,
        is.logical(appendThresholdToTableLabel), length(appendThresholdToTableLabel) == 1L,
        (is.null(overviewImageHeight) || is.numeric(overviewImageHeight))
    )
    
    # Verzeichnisse anlegen
    plotOutPath <- sprintf(
        "%s/Abbildungen/Dreiecksarbitrage/%ds/btc-eur-usd",
        latexOutPath, threshold
    )
    tableOutPath <- sprintf(
        "%s/Tabellen/Dreiecksarbitrage/%ds/btc-eur-usd",
        latexOutPath, threshold
    )
    for (d in c(tableOutPath, plotOutPath)) {
        if (!dir.exists(d)) {
            dir.create(d, recursive=TRUE)
        }
    }
    
    # Lade alle Ergebnissets
    allResults <- NULL
    for (i in seq_along(exchangeNames)) {
        
        # Variablen initialisieren
        exchange <- names(exchangeNames)[[i]]
        exchangeName <- exchangeNames[[i]]
        
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
        
        # Ergebnis anhängen
        if (!is.null(allResults)) {
            allResults <- rbindlist(list(allResults, result$data))
        } else {
            allResults <- result$data
        }
    }
    
    # Sortieren
    setorder(allResults, Time)
    printf("%s Datensätze insgesamt.\n", format.number(nrow(allResults)))
    
    # Aggregieren
    aggregatedResults <- aggregateResultsByTime(allResults, "1 month")
    
    # Übersichtsgrafik aufbauen
    p_diff <- plotAggregatedResultsOverTime(
        aggregatedResults,
        breakpoints = breakpoints,
        plotTitle = "Arbitrageergebnis"
    )
    p_profitable <- plotProfitableTriplesByTime(
        aggregatedResults,
        breakpoints = breakpoints,
        plotTitle = "Arbitrage-Tripel mit einem Ergebnis von min. 1\\,%, 2\\,% und 5\\,%"
    )
    p_nrow <- plotNumResultsOverTime(
        aggregatedResults,
        breakpoints = breakpoints,
        plotTitle = "Anzahl Beobachtungen"
    )
    p_vola <- plotVolatilityByTime(
        paste0("btc", currency_a),
        paste0("btc", currency_b),
        aggregatedResults[c(1,.N), Time],
        breakpoints = breakpoints,
        plotTitle = "Rollierende, annualisierte 10-Tage-Volatilität der Preise",
        legendBelowGraph = TRUE
    )
    
    # Übersicht ausgeben
    source("Konfiguration/TikZ.R")
    if (is.null(overviewImageHeight)) {
        overviewImageHeight <- 22L
    }
    tikz(
        file = sprintf("%s/Uebersicht_Gesamt.tex", plotOutPath),
        width = documentPageWidth,
        height = overviewImageHeight / 2.54,
        sanitize = TRUE
    )
    print(plot_grid(
        p_diff, p_profitable, p_nrow, p_vola,
        ncol = 1L,
        align = "v"
    ))
    dev.off()
    
    # Weitere Auswertungsgrafiken
    if (!onlyMainGraphAndTable) {
        
        # Überblicksgrafik, Variante mit Preisniveau + Volumen
        p_pricelevel <- plotPriceLevelByTime(
            paste0("btc", currency_a),
            paste0("btc", currency_b),
            aggregatedResults[c(1,.N), Time],
            breakpoints = breakpoints,
            plotTitle = "Preisniveau in US-Dollar bzw. Euro"
        )
        p_volume <- plotTradingVolumeByTime(
            paste0("btc", currency_a),
            paste0("btc", currency_b),
            aggregatedResults[c(1,.N), Time],
            breakpoints = breakpoints,
            plotTitle = "Handelsvolumen in Bitcoin",
            legendBelowGraph = TRUE
        )
        
        tikz(
            file = sprintf("%s/Uebersicht_Gesamt_PreisVolumen.tex", plotOutPath),
            width = documentPageWidth,
            height = 20L / 2.54,
            sanitize = TRUE
        )
        print(plot_grid(
            p_diff, p_profitable, p_pricelevel, p_volume,
            ncol = 1L,
            align = "v"
        ))
        dev.off()
        
        # Boxplot
        plotResultBoxplotByExchange(
            allResults,
            latexOutPath = sprintf("%s/Boxplot_Gesamt.tex", plotOutPath)
        )
        
        # Verteilungs-Histogramm
        plotDistribution(
            allResults,
            latexOutPath = sprintf("%s/Histogramm_Gesamt.tex", plotOutPath)
        )
        
        # Vorteilhafte Route
        plotPercentageBestByRoute(
            allResults,
            latexOutPath = sprintf("%s/Anteile_Bestwert_nach_Route_Gesamt.tex", plotOutPath)
        )
    }
    
    # Überblickstabelle
    if (appendThresholdToTableLabel) {
        tableLabelAppendix <- sprintf(" (Grenzwert %ds)", threshold)
    } else {
        tableLabelAppendix <- ""
    }
    summariseDatasetAsTable(
        allResults,
        outFile = sprintf("%s/Uebersicht_Gesamt.tex", tableOutPath),
        caption = sprintf(
            "Kenngrößen der Dreiecksarbitrage für BTC, EUR und USD%s",
            tableLabelAppendix
        ),
        label = sprintf("Dreiecksarbitrage_BTCEURUSD_%ds_Uebersicht", threshold)
    )
    
    # Keine weitere Auswertung
    if (onlyMainGraphAndTable) {
        return(invisible(NULL))
    }
    
    rm(aggregatedResults)
    
    # Intervalle bestimmen
    intervals <- calculateIntervals(allResults$Time, breakpoints)
    for (segment in seq_len(nrow(intervals))) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        resultSubset <- allResults[Time %between% segmentInterval]
        
        printf(
            "Intervall #%d: %s Datensätze von %s bis %s.\n",
            segment,
            format.number(nrow(resultSubset)),
            format(segmentInterval[1], "%d.%m.%Y"),
            format(segmentInterval[2], "%d.%m.%Y")
        )
        
        # Daten auf eine Woche aggregieren (besser lesbar mit größeren Intervallen)
        aggregatedResultSubset <- aggregateResultsByTime(
            resultSubset,
            floorUnits = "1 week"
        )
        
        p_diff <- plotAggregatedResultsOverTime(
            aggregatedResultSubset,
            plotTitle = "Preisabweichungen"
        )
        
        # Spezielle Grafikkonfiguration abrufen, falls vorhanden
        legendBelowGraph <- plotConfiguration[["profitableTriplesByTime"]][[as.character(segment)]]
        p_profitable <- plotProfitableTriplesByTime(
            aggregatedResultSubset,
            plotTitle = "Arbitrage-Tripel mit einem Ergebnis von min. 1\\,%, 2\\,% und 5\\,%",
            legendBelowGraph = legendBelowGraph
        )
        p_nrow <- plotNumResultsOverTime(
            aggregatedResultSubset,
            plotTitle = "Anzahl Beobachtungen"
        )
        p_vola <- plotVolatilityByTime(
            paste0("btc", currency_a),
            paste0("btc", currency_b),
            aggregatedResultSubset[c(1,.N), Time],
            plotTitle = "Rollierende, annualisierte 10-Tage-Volatilität der Preise",
            legendBelowGraph = TRUE
        )
        
        # Als LaTeX-Dokument ausgeben
        tikz(
            file = sprintf("%s/Uebersicht_%d.tex", plotOutPath, segment),
            width = documentPageWidth,
            height = 22 / 2.54,
            sanitize = TRUE
        )
        print(plot_grid(
            p_diff, p_profitable, p_nrow, p_vola,
            # Plot mit Label unter der Grafik etwas größer machen,
            # damit die Höhe der Zeichenfläche etwa identisch ist
            rel_heights = c(1, 1.15, 1, 1),
            ncol = 1L,
            align = "v"
        ))
        dev.off()
        
        # Boxplot mit Daten des Intervalls
        plotResultBoxplotByExchange(
            resultSubset,
            latexOutPath = sprintf("%s/Boxplot_%d.tex", plotOutPath, segment)
        )
        
        # Verteilungs-Histogramm mit Daten des Intervalls
        largeThresholds <- plotConfiguration[["distribution"]][[as.character(segment)]]
        plotDistribution(
            resultSubset,
            latexOutPath = sprintf("%s/Histogramm_%d.tex", plotOutPath, segment),
            largeThresholds = largeThresholds
        )
        
        # Statistiken in Tabelle ausgeben
        summariseDatasetAsTable(
            resultSubset,
            outFile = sprintf("%s/Uebersicht_%d.tex", tableOutPath, segment),
            caption = sprintf(
                "Kenngrößen der Dreiecksarbitrage (%s bis %s)",
                format(segmentInterval[1], "%B~%Y"),
                format(segmentInterval[2], "%B~%Y")
            ),
            indexCaption = sprintf(
                "Kenngrößen der Dreiecksarbitrage (%s -- %s)",
                format(segmentInterval[1], "%m/%Y"),
                format(segmentInterval[2], "%m/%Y")
            ),
            label = sprintf(
                "Dreiecksarbitrage_BTCEURUSD_%ds_Uebersicht_%d",
                threshold, segment
            ),
            forceTablePosition = forceTablePosition
        )
    }
    
    return(invisible(NULL))
}


# Auswertung (grafisch, numerisch) --------------------------------------------

# Händisch einzeln bei Bedarf starten, da große Datenmengen geladen werden
# und die Verarbeitung viel Zeit in Anspruch nimmt.
if (FALSE) {
    
    for (threshold in thresholds) {
        printf("\n\nBetrachte den Schwellwert %ds...\n", threshold)
        
        if (threshold == mainThreshold) {
            overviewImageHeight <- NULL
        } else {
            overviewImageHeight <- 20L
        }
        
        analyseTriangularArbitrage(
            "usd", "eur",
            threshold,
            onlyMainGraphAndTable = (threshold != mainThreshold),
            appendThresholdToTableLabel = (threshold != mainThreshold),
            overviewImageHeight = overviewImageHeight
        )
    }
}
