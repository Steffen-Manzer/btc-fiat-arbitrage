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
library("patchwork") # Grid-Layout
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
thresholds <- c(1L, 2L, 5L, 10L)

# Haupt-Intervall zur Betrachtung innerhalb der Arbeit
# (Rest: Nur Anhang, reduzierte Ansicht)
mainThreshold <- 1L

# Die Breakpoints selbst werden immer dem letzten der beiden entstehenden
# Intervalle zugerechnet
breakpointsByExchange <- list(
    # Intervall: 1.           2.             3.           4.
    # Von/Bis: --> 2015 -- 2015-2017  --  2017-2019 -- 2019 -->
    "bitfinex" = c(                            "2019-07-01"), # USD ab 14.01.2013 / EUR ab 19.05.2017
    "bitstamp" = c(              "2017-01-01", "2019-07-01"), # USD ab 18.08.2011 / EUR ab 16.04.2016
    "coinbase" = c(              "2017-01-01", "2019-07-01"), # USD ab 01.12.2014 / EUR ab 23.04.2015
    "kraken"   = c("2015-04-01", "2017-01-01", "2019-07-01"), # USD ab 06.10.2013 / EUR ab 10.09.2013
    "all"      = c("2015-04-01", "2017-01-01", "2019-07-01")  # Alle Varianten
)

#' Tabellen-Template mit `{tableContent}`, `{tableCaption` und `{tableLabel}` 
#' als Platzhalter
summaryTableTemplateFile <- 
    sprintf("%s/Tabellen/Templates/Dreiecksarbitrage_Uebersicht_nach_Boerse.tex",
            latexOutPath)


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
#' @param result Eine Instanz der Klasse `TriangularResult`
#' @param floorUnits Aggregations-Zeitfenster, genutzt als `unit` für
#'   `floor_date`. Werte kleiner als ein Tag sind grafisch kaum darstellbar.
#' @param interval Nur dieses Intervall berücksichtigen
#' @return `data.table` mit Min, Q1, Median, Mean, Q3, Max
aggregateResultsByTime <- function(result, floorUnits, interval = NULL)
{
    # Parameter validieren
    stopifnot(
        inherits(result, "TriangularResult"),
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
        
        # Wieviele Preistripel sind größer als...
        # 1 %
        nLargerThan1Pct = length(which(BestResult >= .01)),
        # 2 %
        nLargerThan2Pct = length(which(BestResult >= .02)),
        # 5 %
        nLargerThan5Pct = length(which(BestResult >= .05)),
        
        # Routen-Statistiken: Wie oft ist Route 1 / 2 die bessere Wahl?
        AB_Best = sum(as.integer(ResultAB == BestResult)) / .N,
        BA_Best = sum(as.integer(ResultBA == BestResult)) / .N
    ))
    
    if (!is.null(interval)) {
        
        # Zeitraum begrenzt
        aggregatedResults <- result$data[
            Time %between% interval,
            j = eval(group),
            by = .(Time=floor_date(Time, unit=floorUnits))
        ]
        
    } else {
        
        # Gesamte Daten
        aggregatedResults <- result$data[
            j = eval(group),
            by = .(Time=floor_date(Time, unit=floorUnits))
        ]
    }
    
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
        !is.null(arbitrageResults$Time),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L)
    )
    
    # Ausgabeoptionen
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    # Einige Bezeichnungen und Variablen
    plotTextPrefix <- "\\footnotesize "
    plotSmallPrefix <- "\\small "
    plotXLab <- "Datum"
    plotYLab <- "Arbitrageergebnis"
    
    # Zeichnen
    if (!is.null(arbitrageResults$Exchange)) {
        minValue <- min(arbitrageResults$Median)
        maxValue <- max(arbitrageResults$Median)
    } else if (!is.null(arbitrageResults$Q3)) {
        minValue <- min(arbitrageResults$Q1)
        maxValue <- max(arbitrageResults$Q3)
    } else {
        stop("Keine Quartile und keine Rohdaten gefunden!")
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
    
    if (is.null(arbitrageResults$Exchange)) {
        
        # Quartilswerte + Median für Einzelbörsen
        plot <- plot +
            # Q1/Q3 zeichnen
            geom_ribbon(aes(x=Time, ymin=Q1, ymax=Q3), fill="grey70") +
            
            # Median zeichnen
            geom_line(aes(x=Time, y=Median, color="1", linetype="1"), size=.5, show.legend=FALSE) +
            
            # Normales "bright"-Farbschema
            scale_color_bright()
        
    } else {
        
        # Median für Überblick
        plot <- plot +
            geom_line(
                aes(x=Time, y=Median, color=Exchange, linetype=Exchange),
                size = .5
            ) +
            
            # Farbreihenfolge für bessere Lesbarkeit hier ausnahmsweise ändern
            scale_color_manual(
                values = rev(unname(colour("bright")(4))),
                limits = unlist(unname(exchangeNames))
            )
        
    }
    
    plot <- plot +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            # Legende immer rechts ausrichten. Für Einzel-Börsen ohnehin keine Legende.
            legend.position = "right"
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
        # Für das "Set" der breakpoints keine Legende zeigen
        scale_fill_pale(guide="none") +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab),
            color = paste0(plotTextPrefix, "Börse"),
            linetype = paste0(plotTextPrefix, "Börse")
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0(plotSmallPrefix, plotTitle))
    }
    
    if (!is.null(latexOutPath)) {
        # Plot zeichnen
        print(plot)
        dev.off()
        return(invisible(plot))
    } else {
        return(plot)
    }
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
    plotTextPrefix <- "\\footnotesize "
    plotSmallPrefix <- "\\small "
    
    # Ausgabeoptionen
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.R")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
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
    
    # Werteachse bestimmen
    if (maxValue < .02) {
        numYDigits <- 1L
    } else {
        numYDigits <- 0L
    }
    
    # Legende einrichten
    if (isTRUE(legendBelowGraph)) {
        legendPosition <- "bottom"
        legendMargin <- margin(t=-5)
        legendBackground <- element_blank()
    } else if (!is.null(result$Exchange)) {
        legendPosition <- "right"
        legendMargin <- margin()
        legendBackground <- element_blank()
    } else if (is.null(breakpoints)) {
        legendPosition <- c(0.88, 0.7)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    } else {
        legendPosition <- c(0.88, 0.58)
        legendMargin <- margin(0, 5, 5, 5)
        legendBackground <- element_rect(fill="white", size=0.2, linetype="solid")
    }
    
    if (is.null(result$Exchange)) {
        
        # Einzelbörse
        plot <- plot +
            geom_line(aes(x=Time, y=nLargerThan1Pct/n, color="1\\,%", linetype="1\\,%"), size = .5) +
            geom_line(aes(x=Time, y=nLargerThan2Pct/n, color="2\\,%", linetype="2\\,%"), size = .5) +
            geom_line(aes(x=Time, y=nLargerThan5Pct/n, color="5\\,%", linetype="5\\,%"), size = .5) +
            
            # Normales "bright"-Farbschema
            scale_color_bright()
        
        legendName <- "Grenzwert"
        
    } else {
        
        # Übersicht
        plot <- plot +
            geom_line(
                aes(x=Time, y=nLargerThan1Pct/n, color=Exchange, linetype=Exchange),
                size = .5
            ) +
            
            # Farbreihenfolge für bessere Lesbarkeit hier ausnahmsweise ändern
            scale_color_manual(
                values = rev(unname(colour("bright")(4))),
                limits=unlist(unname(exchangeNames))
            )
        
        legendName <- "Börse"
        
    }
    
    # Grafik mit Anteilen zeichnen
    plot <- plot +
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
            labels = function(x) paste0(format.percentage(x, digits=numYDigits), "\\,%")
        ) +
        # Für das "Set" der breakpoints keine Legende zeigen
        scale_fill_pale(guide="none") +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab),
            linetype = paste0(plotTextPrefix, legendName),
            colour = paste0(plotTextPrefix, legendName)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0(plotSmallPrefix, plotTitle))
    }
    
    if (!is.null(latexOutPath)) {
        # Plot zeichnen
        print(plot)
        dev.off()
        return(invisible(plot))
    } else {
        return(plot)
    }
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
        roundedTo <- "Mio."
        roundFac <- 1e6
    } else {
        roundedTo <- "Tsd."
        roundFac <- 1e3
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
    
    if (is.null(arbitrageResults$Exchange)) {
        groupValue <- expr("1")
        showLegend <- FALSE
        plot <- plot + scale_color_bright()
    } else {
        groupValue <- expr(Exchange)
        showLegend <- TRUE
        plot <- plot +
            # Farbreihenfolge für bessere Lesbarkeit hier ausnahmsweise ändern
            scale_color_manual(
                values = rev(unname(colour("bright")(4))),
                limits=unlist(unname(exchangeNames))
            )
    }
    
    # Anzahl Datensätze zeichnen
    plot <- plot +
        geom_line(
            aes(x=Time, y=n, color=eval(groupValue), linetype=eval(groupValue)),
            size = .5,
            show.legend = showLegend
        )  + 
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0)),
            # Legende immer rechts ausrichten. Für Einzel-Börsen ohnehin keine Legende.
            legend.position = "right"
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
        # Für das "Set" der breakpoints keine Legende zeigen
        scale_fill_pale(guide="none") +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, " [", roundedTo, "]"),
            color = paste0(plotTextPrefix, "Börse"),
            linetype = paste0(plotTextPrefix, "Börse")
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
#' @param pair2 Das gewünschte Kurspaar, bspw. btcusd
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
        # Für das "Set" der breakpoints keine Legende zeigen
        scale_fill_pale(guide="none") +
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


#' Informationen über einen Datensatz für eine Gesamtauswertung
#' (bspw. in Tabellenform) sammeln
#' 
#' @param result Eine Instanz der Klasse `TriangularResult`
#' @return data.table Tabelle mit Gesamtstatistiken
collectDatasetSummary <- function(result)
{
    stopifnot(inherits(result, "TriangularResult"))
    
    numRows <- nrow(result$data)
    intervalLengthHours <- 
        difftime(
            last(result$data$Time),
            first(result$data$Time),
            units = "hours"
        ) |>
        round() |>
        as.double()
    
    return(data.table(
        exchange = result$Exchange,
        exchangeName = result$ExchangeName,
        numRows = numRows,
        firstResult = first(result$data$Time),
        lastResult = last(result$data$Time),
        intervalLengthHours = intervalLengthHours,
        numRowsPerHour = numRows / intervalLengthHours,
        numRowsPerDay = numRows / (intervalLengthHours / 24),
        numRowsLargerThan_A = length(which(result$data$BestResult >= .01)),
        numRowsLargerThan_B = length(which(result$data$BestResult >= .02)),
        numRowsLargerThan_C = length(which(result$data$BestResult >= .05)),
        maxValue = max(result$data$BestResult)
    ))
}


#' Informationen über einen Datensatz als LaTeX-Tabelle ausgeben
#' 
#' @param dataset `data.table`, eine Kollektion aus `collectDatasetSummary`
#' @param outFile Zieldatei
#' @param caption Tabellentitel
#' @param label Tabellenlabel
summariseDatasetAsTable <- function(
    dataset,
    outFile = NULL,
    caption = NULL,
    label = NULL
)
{
    printf("Erzeuge Überblickstabelle in %s...\n", basename(outFile))
    numRowsTotal <- sum(dataset$numRows)
    
    # Tabellenzeile erzeugen
    createRow <- function(dataSubset, end="\\\\\n\n") {
        s <- strrep(" ", 12) # Einrückung in der Ergebnisdatei
        return(paste0(
            sprintf("%s%% %s Datensätze pro Tag\n", s,
                    format.numberWithFixedDigits(dataSubset$numRowsPerDay, digits=1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(dataSubset$numRows),
                    format.percentage(dataSubset$numRows / numRowsTotal, 1L)),
            sprintf("%s%s &\n", s, format.numberWithFixedDigits(dataSubset$numRowsPerHour, 1)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(dataSubset$numRowsLargerThan_A),
                    format.percentage(dataSubset$numRowsLargerThan_A / dataSubset$numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(dataSubset$numRowsLargerThan_B),
                    format.percentage(dataSubset$numRowsLargerThan_B / dataSubset$numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(dataSubset$numRowsLargerThan_C),
                    format.percentage(dataSubset$numRowsLargerThan_C / dataSubset$numRows, 1L)),
            sprintf("%s%s\\,\\%% ", s, 
                    format.percentage(dataSubset$maxValue, 1)),
            end
        ))
    }
    
    # Tabelleninhalt initialisieren
    tableContent <- ""
    
    # Tabelle kann im Fließtext unterkommen
    tablePosition <- "tbh"
    
    # Jede Börse durchgehen
    for (i in seq_len(nrow(dataset))) {
        dataSubset <- dataset[i]
        tableContent <- paste0(
            tableContent,
            "\n",
            sprintf("        %s &\n", dataSubset$exchangeName),
            createRow(dataSubset)
        )
    }
    
    # Gesamtdaten berechnen
    tableContent <- paste0(
        tableContent,
        "        \\tablebody\n\n",
        "        \\rowcolor{white}\n",
        "        Gesamt &\n",
        createRow(data.table(
            numRows = numRowsTotal,
            numRowsPerHour = numRowsTotal / max(dataset$intervalLengthHours),
            numRowsPerDay = numRowsTotal / (max(dataset$intervalLengthHours) / 24),
            numRowsLargerThan_A = sum(dataset$numRowsLargerThan_A),
            numRowsLargerThan_B = sum(dataset$numRowsLargerThan_B),
            numRowsLargerThan_C = sum(dataset$numRowsLargerThan_C),
            maxValue = max(dataset$maxValue)
        ))
    )
    
    # Tabelle einfach ausgeben
    if (is.null(outFile)) {
        printf(tableContent)
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
#' @param overviewImageHeight Höhe der Überblicksgrafik überschreiben
#' @return `data.table`: Ergebnis von `collectDatasetSummary()`
analyseTriangularArbitrage <- function(
    currency_a,
    currency_b,
    threshold,
    overviewImageHeight = NULL
)
{
    # Parameter validieren
    stopifnot(
        is.character(currency_a), length(currency_a) == 1L, nchar(currency_a) == 3L,
        is.character(currency_b), length(currency_b) == 1L, nchar(currency_b) == 3L,
        is.numeric(threshold), length(threshold) == 1L,
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
    
    # LaTeX/TikZ-Ausgabemodul laden
    source("Konfiguration/TikZ.R")
    if (is.null(overviewImageHeight)) {
        overviewImageHeight <- 22L
    }
    
    # Alle Ergebnisse für Gesamtauswertung zwischenspeichern
    summaryStatistics <- data.table()
    allAggregatedResults <- data.table()
    
    # Einzelauswertung
    for (i in seq_along(exchangeNames)) {
        
        # Variablen initialisieren
        exchange <- names(exchangeNames)[[i]]
        exchangeName <- exchangeNames[[i]]
        breakpoints <- breakpointsByExchange[[exchange]]
        
        dataFile <- sprintf(
            "Cache/Dreiecksarbitrage/%ds/%s-%s-%s.fst",
            threshold, exchange, currency_a, currency_b
        )
        stopifnot(file.exists(dataFile))
        
        # Daten einlesen
        printf("Lese Daten für %s...\n", exchangeName)
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
        
        # Zusammenfassung in Konsole ausgeben
        numTotal <- nrow(result$data)
        printf("%s Datensätze, davon ", format.number(numTotal))
        for (largerThan in c(1, 2, 5)) {
            numLarger <- length(result$data[BestResult >= (largerThan / 100), which=TRUE])
            printf(
                "%s (%s %%) >= %d %%, ",
                format.number(numLarger), format.percentage(numLarger/numTotal, 1L), largerThan
            )
        }
        printf("\n")
        
        # Monatsdaten berechnen
        aggregatedResults <- aggregateResultsByTime(result, "1 month")
        
        # Überblicksgrafik
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
            exchange,
            breakpoints = breakpoints,
            plotTitle = "Rollierende, annualisierte 10-Tage-Volatilität der Preise",
            legendBelowGraph = TRUE
        )
        
        # Übersicht ausgeben
        tikz(
            file = sprintf("%s/%s/Uebersicht.tex", plotOutPath, exchange),
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
        
        # Gesamtstatistiken aufbauen
        aggregatedResults[, Exchange:=exchangeName]
        allAggregatedResults <- rbindlist(list(allAggregatedResults, aggregatedResults))
        summaryStatistics <- rbindlist(list(summaryStatistics, collectDatasetSummary(result)))
    }
    
    # Gesamtübersichtsgrafik aufbauen
    setorder(allAggregatedResults, Time, Exchange)
    breakpoints <- breakpointsByExchange[["all"]]
    
    p_diff <- plotAggregatedResultsOverTime(
        allAggregatedResults, 
        
        # Kraken hat die meisten Breakpoints
        breakpoints = breakpoints,
        plotTitle = "Arbitrageergebnis"
    )
    p_profitable <- plotProfitableTriplesByTime(
        allAggregatedResults,
        breakpoints = breakpoints,
        plotTitle = "Arbitrage-Tripel mit einem Ergebnis von min. 1\\,%"
    )
    p_nrow <- plotNumResultsOverTime(
        allAggregatedResults,
        breakpoints = breakpoints,
        plotTitle = "Anzahl Beobachtungen"
    )
    p_vola <- plotVolatilityByTime(
        paste0("btc", currency_a),
        paste0("btc", currency_b),
        allAggregatedResults[c(1,.N), Time],
        breakpoints = breakpoints,
        plotTitle = "Rollierende, annualisierte 10-Tage-Volatilität der Preise"
    )
    
    # Übersicht ausgeben
    tikz(
        file = sprintf("%s/Uebersicht.tex", plotOutPath),
        width = documentPageWidth,
        height = overviewImageHeight / 2.54,
        sanitize = TRUE
    )
    #' Nutze `patchwork` statt `cowplot`, da sonst die Legende zu breit wird
    print(p_diff / p_profitable / p_nrow / p_vola)
    dev.off()
    
    # Zusammenfassende Tabelle erstellen
    summariseDatasetAsTable(
        summaryStatistics,
        outFile = sprintf("%s/Uebersicht.tex", tableOutPath),
        caption = sprintf(
            "Zentrale Kenngrößen der Dreiecksarbitrage für BTC, EUR und USD im Gesamtüberblick"
        ),
        label = sprintf("Dreiecksarbitrage_BTCEURUSD_Uebersicht_%ds", threshold)
    )
}


# Auswertung (grafisch, numerisch) --------------------------------------------

# Händisch einzeln bei Bedarf starten, da große Datenmengen geladen werden
# und die Verarbeitung viel Zeit in Anspruch nimmt.
if (FALSE) {
    
    thresholds <- c(1L) # Testmodus/Entwicklungsmodus
    for (threshold in thresholds) {
        printf("\n\nBetrachte den Schwellwert %ds...\n", threshold)
        
        if (threshold == mainThreshold) {
            overviewImageHeight <- NULL
        } else {
            overviewImageHeight <- 20L
        }
        
        analyseTriangularArbitrage("usd", "eur", threshold, overviewImageHeight)
    }
}
