#' Auswertung der Preisabweichungen von Bitcoin-Börsen mit Blick auf
#' Möglichkeiten der Raumarbitrage.
#'
#' Notwendig ist die vorherige paarweise Berechnung und Speicherung relevanter
#' Preisabweichungen unter
#'   `Cache/Raumarbitrage/{Grenzwert}s/{Kurspaar}-{Börse 1}-{Börse 2}-{i}`
#'   mit `i = 1 ... n`.
#' über die Datei `Bitcoin - Abweichungen berechnen.r`.
#'
#' Stand Januar 2022 passen sämtliche Ergebnisse noch mit etwas Puffer in eine
#' einzelne Ergebnisdatei, ohne dass diese jeweils mehr als 4 GB Arbeitsspeicher
#' belegen würde.
#' 
#' Diese Auswertung lädt aus diesem Grund derzeit je Kurs-/Börsenpaar nur die
#' erste Ergebnisdatei und prüft nicht, ob weitere Ergebnisse vorliegen.
#' Das Nachladen weiterer Ergebnisdateien müsste in Zukunft ergänzt werden, wenn
#' einzelne Dateien die Schwelle von 100 Mio. Datensätzen überschreiten.
#' 
#' **Anmerkung:**
#' Für die deckenden Hintergründe der hier erstellten Grafiken wird
#' abweichend von den sonst genutzten Farben ein anderes Farbschema von Paul Tol
#' (definiert im Paket `khroma`) verwendet, das besser als Hintergrund geeignet ist.
#' 
#' Im Panel
#'   "Anteil der Arbitrage-Paare mit einer Abweichung von min. 1 %, 2 % und 5 %",
#' erstellt in der Methode
#'   `plotProfitableDifferencesByTime`,
#' wird zusätzlich für die Vordergrundfarben abweichend das Farbschema
#'   **bright**
#' aus dem selben Paket herangezogen.
#' Darüber hinaus bleibt das Farbschema der Vordergrundfarben identisch.


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Funktionen/CalculateIntervals.R")
source("Funktionen/FormatCurrencyPair.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
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
library("stringr") # str_replace
library("tictoc")
library("TTR") # volatility


# Konfiguration ---------------------------------------------------------------
plotAsLaTeX <- FALSE
exchangeNames <- list(
    "bitfinex" = "Bitfinex",
    "bitstamp" = "Bitstamp",
    "coinbase" = "Coinbase Pro",
    "kraken" = "Kraken"
)

# Verfügbare Währungspaare. BTC/USD: ~11,5 GB, BTC/EUR: ~4,5 GB
currencyPairs <- c("btcusd", "btceur")

# Verfügbare Grenzwerte für Abschluss des Arbitragegeschäfts in Sekunden
thresholds <- c(2L, 5L, 10L)

# Haupt-Intervall zur Betrachtung innerhalb der Arbeit
# (Rest: Nur Anhang, reduzierte Ansicht)
mainThreshold <- 2L

# Breakpoints für eine detailliertere Betrachtung einzelner Zeitabschnitte
# Die Breakpoints selbst werden immer dem letzten der beiden entstehenden
# Intervalle zugerechnet
breakpointsByCurrency <- list(
    # Intervall: 1.           2.             3.           4.
    # Von/Bis: -> 2015 -- 2015-2017  --  2017-2019 -- 2019 ->
    "btcusd" = c("2015-04-01", "2017-01-01", "2019-07-01"),
    "btceur" = c(              "2017-01-01", "2019-07-01")
)

# Spezielle Plot-Optionen für einzelne Intervalle
plotConfiguration <- list(
    # Plot: Anteil Abweichungen > 1/2/5%
    "profitableDifferencesByTime" = list(
        # Paar: BTC/USD
        "btcusd" = list(
            # Segmente 1, 2 und 3: Legende unter der Grafik anzeigen
            "1" = TRUE,
            "2" = TRUE,
            "3" = TRUE
        )
    )
)

#' Tabellen-Template mit `{tableContent}`, `{tableCaption}` und `{tableLabel}` 
#' als Platzhalter
summaryTableTemplateFile <- 
    sprintf("%s/Tabellen/Templates/Raumarbitrage_Uebersicht_nach_Boerse.tex",
            latexOutPath)

# Schnellstart für Entwicklung
pair <- "btcusd"
breakpoints <- breakpointsByCurrency[[pair]]
threshold <- mainThreshold
overviewImageHeight <- NULL
forceTablePosition <- NULL
onlyMainGraphAndTable <- FALSE
appendThresholdToTableLabel <- FALSE


# Hilfsfunktionen -------------------------------------------------------------

#' Lade alle vergleichbaren Preise für ein Kurspaar
#' 
#' @param currencyPair Kurspaar (z.B. BTCUSD)
#' @param threshold Zeitliche Differenz zweier Ticks in Sekunden,
#'                  ab der das Tick-Paar verworfen wird.
#' @return `data.table` mit den Preisabweichungenn
loadComparablePricesByCurrencyPair <- function(currencyPair, threshold)
{
    # Parameter validieren
    stopifnot(
        is.character(currencyPair), length(currencyPair) == 1L, nchar(currencyPair) == 6L,
        is.numeric(threshold), length(threshold) == 1L
    )
    
    # Nur jeweils erste Datei einlesen, siehe oben.
    printf("Lade alle paarweisen Preise für %s (Schwellwert: %ds).\n", currencyPair, threshold)
    sourceFiles <- list.files(
        sprintf("Cache/Raumarbitrage/%ds", threshold),
        pattern = sprintf("^%s-.*-1\\.fst$", currencyPair),
        full.names = TRUE
    )
    
    # Variablen initialisieren / leeren
    combinedPriceDifferences <- NULL
    
    # Alle Datensätze einlesen
    for (i in seq_along(sourceFiles)) {
        
        # Dateiname bestimmen
        sourceFile <- sourceFiles[[i]]
        
        # Datei lesen
        priceDifferences <- read_fst(
            sourceFile,
            columns = c("Time", "PriceHigh", "ExchangeHigh", "PriceLow", "ExchangeLow"),
            as.data.table = TRUE
        )
        
        # Unterschiede berechnen
        priceDifferences[,PriceDifference:=(PriceHigh/PriceLow)-1]
        
        # Statistiken ausgeben
        printf(
            "%s (%s) mit %s Datensätzen von %s bis %s.\n", 
            basename(sourceFile),
            format(object.size(priceDifferences), units="auto", standard="SI"),
            format.number(nrow(priceDifferences)),
            format(first(priceDifferences$Time), "%d.%m.%Y"),
            format(last(priceDifferences$Time), "%d.%m.%Y")
        )
        maxDifference <- priceDifferences[which.max(PriceDifference)]
        with(maxDifference, printf(
            "Höchstwert: %s (%s <-> %s) am %s\n",
            format.number(PriceDifference),
            format.money(PriceLow),
            format.money(PriceHigh),
            formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
        ))
        
        if (!is.null(combinedPriceDifferences)) {
            # Ergebnisse anhängen
            combinedPriceDifferences <- rbindlist(
                list(combinedPriceDifferences, priceDifferences)
            )
        } else {
            # Erste Datei
            combinedPriceDifferences <- priceDifferences
        }
        
        # Speicher freigeben
        rm(priceDifferences)
        gc()
    }
    
    # Sortieren
    setorder(combinedPriceDifferences, Time)
    
    # Statistiken ausgeben
    printf(
        "\nKombination aller Börsen (%ds) ergab %s Datensätze (%s) von %s bis %s.\n",
        threshold,
        format.number(nrow(combinedPriceDifferences)),
        format(object.size(combinedPriceDifferences), units="auto", standard="SI"),
        format(first(combinedPriceDifferences$Time), "%d.%m.%Y"),
        format(last(combinedPriceDifferences$Time), "%d.%m.%Y")
    )
    
    return(combinedPriceDifferences)
}


#' Aggregiere auf den angegebenen Zeitraum
#' 
#' @param comparablePrices `data.table` mit den Preisen der verschiedenen Börsen
#' @param floorUnits Aggregations-Zeitfenster, genutzt als `unit` für
#'   `floor_date`. Werte kleiner als ein Tag sind grafisch kaum darstellbar.
#' @return `data.table` mit Q1, Median, Mean, Q3, Max
aggregatePriceDifferences <- function(
    comparablePrices,
    floorUnits
) {
    # Parameter validieren
    stopifnot(
        is.data.table(comparablePrices), nrow(comparablePrices) > 0L,
        !is.null(comparablePrices$Time), !is.null(comparablePrices$PriceDifference),
        is.character(floorUnits), length(floorUnits) == 1L
    )
    
    tic()
    
    # Aggregation aller gefundener Tauschmöglichkeiten
    priceDifferences <- comparablePrices[
        j=.(
            Q1 = quantile(PriceDifference, probs=.25, names=FALSE),
            Mean = mean(PriceDifference),
            Median = median(PriceDifference),
            Q3 = quantile(PriceDifference, probs=.75, names=FALSE),
            Max = max(PriceDifference),
            n = .N,
            
            # Wieviele Preisunterschiede sind größer als...
            # 1 %
            nLargerThan1Pct = length(which(PriceDifference >= .01)),
            # 2 %
            nLargerThan2Pct = length(which(PriceDifference >= .02)),
            # 5 %
            nLargerThan5Pct = length(which(PriceDifference >= .05))
        ),
        by=.(Time=floor_date(Time, unit=floorUnits))
    ]
    printf(
        "Aggregation auf '%s' ergab %s Datensätze. ",
        floorUnits, format.number(nrow(priceDifferences))
    )
    toc()
    
    return(priceDifferences)
}


#' Zeichne Preisabweichungen als Linien-/Punktgrafik im Zeitverlauf
#' 
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param removeGaps Datenlücken nicht interpolieren/zeichnen
#' @param plotType Plot-Typ: line oder point
#' @param plotTitle Überschrift (optional)
#' @return Der Plot (unsichtbar)
plotAggregatedPriceDifferencesByTime <- function(
    priceDifferences,
    latexOutPath = NULL,
    breakpoints = NULL,
    removeGaps = TRUE,
    plotType = "line",
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences), nrow(priceDifferences) > 0L, !is.null(priceDifferences$Time),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.logical(removeGaps), length(removeGaps) == 1L,
        is.character(plotType), length(plotType) == 1L,
        is.null(plotTitle) || (is.character(plotTitle) && length(plotTitle) == 1L)
    )
    
    # Einige Bezeichnungen und Variablen
    plotTextPrefix <- "\\footnotesize "
    plotSmallPrefix <- "\\small "
    plotXLab <- "Datum"
    plotYLab <- "Preisabweichung"
    
    # Zeichnen
    if (!is.null(priceDifferences$Q3)) {
        maxValue <- max(priceDifferences$Q3)
    } else if (!is.null(priceDifferences$PriceDifference)) {
        maxValue <- max(priceDifferences$PriceDifference)
    } else {
        stop("Keine Quartile und keine Rohdaten gefunden!")
    }
    
    plot <- ggplot(priceDifferences)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(priceDifferences$Time, breakpoints)
        
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
    
    if (plotType == "line") {
        
        # Liniengrafik. Nützlich, wenn Daten nahezu kontinuierlich vorliegen
        # Bestehende Lücken > 2 Tage dennoch auslassen und nicht interpolieren
        # https://stackoverflow.com/a/21529560
        # TODO Variabel gestalten (nicht fix 2 Tage)?
        if (removeGaps) {
            gapGroups <- c(0, cumsum(diff(priceDifferences$Time) > 2))
        } else {
            gapGroups <- 0
        }
        
        plot <- plot +
            # Q1/Q3 zeichnen
            geom_ribbon(aes(x=Time, ymin=Q1, ymax=Q3, group=gapGroups), fill="grey70") +
            
            # Median zeichnen
            geom_line(aes(x=Time, y=Median, color="1", linetype="1", group=gapGroups), size=.5)
        
    } else if (plotType == "point") {
        
        # Punkt-Grafik. Besser, wenn Daten viele Lücken aufweisen
        plot <- plot +
            geom_point(aes(x=Time, y=PriceDifference, color="1"), size=.25)
        
    } else {
        stop(sprintf("Unbekannter Plot-Typ: %s", plotType))
    }
    
    # Datumsachse bestimmen
    if (difftime(max(priceDifferences$Time), min(priceDifferences$Time), units = "weeks") > 2*52) {
        date_breaks <- "1 year"
        date_labels <- "%Y"
    } else {
        date_breaks <- expr(waiver())
        date_labels <- "%m/%Y"
    }
    
    plot <- plot + 
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(
            labels = function(x) paste(format.number(x * 100), "%")
        ) +
        # Ähnlich wie scale_color_ptol, aber mit höherem Kontrast
        scale_color_highcontrast() +
        # Kompromiss aus guter Erkennbarkeit bei SW + Farbe als Hintergrund
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(
            paste0(plotSmallPrefix, plotTitle)#,
            #subtitle="\\small Median, 25\\,%- und 75\\,%-Quartil der Monatsdaten"
        )
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
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param plotTitle Überschrift (optional)
#' @return Der Plot (unsichtbar)
plotNumDifferencesByTime <- function(
    priceDifferences,
    breakpoints = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences), nrow(priceDifferences) > 0L, !is.null(priceDifferences$Time),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (is.character(plotTitle) && length(plotTitle) == 1L)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- "Beobachtungen"
    plotTextPrefix <- "\\footnotesize "
    maxValue <- max(priceDifferences$n)
    
    # Achseneigenschaften
    if (maxValue > 1e6) {
        roundedTo <- "Mio."
        roundFac <- 1e6
    } else {
        roundedTo <- "Tsd."
        roundFac <- 1e3
    }
    
    plot <- ggplot(priceDifferences)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(priceDifferences$Time, breakpoints)
        
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
    if (difftime(max(priceDifferences$Time), min(priceDifferences$Time), units = "weeks") > 2*52) {
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
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
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
        # Ähnlich wie scale_color_ptol, aber mit höherem Kontrast
        scale_color_highcontrast() +
        # Kompromiss aus guter Erkennbarkeit bei SW + Farbe als Hintergrund
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, " [", roundedTo, "]")
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Anteil der "vorteilhaften" Preisabweichungen im Zeitverlauf darstellen
#' 
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Zieldatei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param plotTitle Überschrift (optional)
#' @param legendBelowGraph Position der Legende (optional)
#' @return Der Plot (unsichtbar)
plotProfitableDifferencesByTime <- function(
    priceDifferences,
    latexOutPath = NULL,
    breakpoints = NULL,
    plotTitle = NULL,
    legendBelowGraph = NULL
)
{
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (is.character(plotTitle) && length(plotTitle) == 1L),
        is.null(legendBelowGraph) || (is.logical(legendBelowGraph) && length(legendBelowGraph) == 1L)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- "Anteil"
    plotTextPrefix <- "\\footnotesize "
    
    # Gesamtübersicht, gruppiert nach Grenzwerten, aggregiert
    plot <- ggplot(priceDifferences)
    
    # Maximalwert entsteht immer bei der geringsten Abweichung (Hier: 1%)
    maxValue <- with(priceDifferences, max(nLargerThan1Pct / n))
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(priceDifferences$Time, breakpoints)
        
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
    if (difftime(max(priceDifferences$Time), min(priceDifferences$Time), units = "weeks") > 2*52) {
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
        legendPosition <- c(0.88, 0.58)
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
            legend.title = element_blank(),
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(
            labels = function(x) paste0(format.percentage(x, digits=0), "\\,%")
        ) +
        # Vor dem farbigen Hintergrund ist das "bright"-Schema am besten erkennbar.
        # Resultierende kräftige Linienfarben: Blau, Rot, Grün
        scale_color_bright() +
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab),
            linetype = "\\footnotesize Grenzwert",
            colour = "\\footnotesize Grenzwert"
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


#' Zeichne das gehandelte Volumen eines Kurspaares an allen Börsen für den
#' angegebenen Zeitabschnitt.
#' 
#' @param pair Das gewünschte Kurspaar, bspw. btcusd
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @return Plot
plotTradingVolumeByTime <- function(
    pair,
    timeframe,
    breakpoints = NULL,
    plotTitle = NULL,
    aggregationLevel = "monthly"
)
{
    stopifnot(
        is.character(pair), length(pair) == 1L,
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
    exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")
    result <- data.table()
    for (exchange in exchanges) {
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-%3$s.fst",
            tolower(exchange), tolower(pair), aggregationSourceLevel
        )
        
        if (!file.exists(sourceFile)) {
            # Dieses Paar wird an dieser Börse nicht gehandelt
            next
        }
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Amount"), as.data.table=TRUE)
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
    }
    
    if (nrow(result) == 0L) {
        stop(sprintf(
            "Datensatz (Handelsvolumen) ist leer. Daten für Aggregationslevel %s vorhanden?",
            aggregationLevel
        ))
    }
    
    if (aggregationLevel == "weekly") {
        # Auf Wochendaten aggregieren
        result <- result[j=.(Amount=sum(Amount)), by=.(Time=floor_date(Time, "1 week"))]
    } else {
        # Direkt zusammenfassen
        result <- result[j=.(Amount=sum(Amount)), by=Time]
    }
    setorder(result, Time)
    
    # Achseneigenschaften
    maxValue <- max(result$Amount)
    if (maxValue > 1e6) {
        roundedTo <- " [Mio. BTC]"
        roundFac <- 1e6
    } else if (maxValue > 1e3) {
        roundedTo <- " [Tsd. BTC]"
        roundFac <- 1e3
    } else {
        roundedTo <- " [BTC]"
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
        geom_line(aes(x=Time, y=Amount, color="1", linetype="1"), size=.5) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(labels = function(x) format.number(x/roundFac)) +
        # Ähnlich wie scale_color_ptol, aber mit höherem Kontrast
        scale_color_highcontrast() +
        # Kompromiss aus guter Erkennbarkeit bei SW + Farbe als Hintergrund
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, roundedTo)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Preisniveau (Mittelwert) eines Kurspaares an allen Börsen für den
#' angegebenen Zeitabschnitt
#' 
#' @param pair Das gewünschte Kurspaar, bspw. btcusd
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @return Plot
plotPriceLevelByTime <- function(
    pair,
    timeframe,
    breakpoints = NULL,
    plotTitle = NULL,
    aggregationLevel = "weekly"
)
{
    stopifnot(
        is.character(pair), length(pair) == 1L,
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
    units <- pair |> toupper() |> substr(4, 6)
    
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
    exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")
    result <- data.table()
    for (exchange in exchanges) {
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-%3$s.fst",
            tolower(exchange), tolower(pair), aggregationSourceLevel
        )
        
        if (!file.exists(sourceFile)) {
            # Dieses Paar wird an dieser Börse nicht gehandelt
            next
        }
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Mean"), as.data.table=TRUE)
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
    }
    
    if (nrow(result) == 0L) {
        stop(sprintf(
            "Datensatz ist leer. Daten für Aggregationslevel %s vorhanden?",
            aggregationLevel
        ))
    }
    
    if (aggregationLevel == "weekly") {
        # Auf Wochendaten aggregieren
        result <- result[j=.(Mean=mean(Mean)), by=.(Time=floor_date(Time, "1 week"))]
    } else {
        # Direkt zusammenfassen
        result <- result[j=.(Mean=mean(Mean)), by=Time]
    }
    setorder(result, Time)
    
    # Achseneigenschaften
    maxValue <- max(result$Mean)
    if (maxValue > 1e6) {
        roundedTo <- sprintf(" [Mio. %s]", units)
        roundFac <- 1e6
    } else if (maxValue > 1e3) {
        roundedTo <- sprintf(" [Tsd. %s]", units)
        roundFac <- 1e3
    } else {
        roundedTo <- sprintf(" [%s]", units)
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
        geom_line(aes(x=Time, y=Mean, color="1", linetype="1"), size=.5) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(labels = function(x) format.number(x/roundFac)) +
        # Ähnlich wie scale_color_ptol, aber mit höherem Kontrast
        scale_color_highcontrast() +
        # Kompromiss aus guter Erkennbarkeit bei SW + Farbe als Hintergrund
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, roundedTo)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Volatilität (Mittelwert) eines Kurspaares an allen Börsen für den
#' angegebenen Zeitabschnitt
#' 
#' @param pair Das gewünschte Kurspaar, bspw. btcusd
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @param n Anzahl Perioden für die Vola.-Abschätzung
#' @return Plot
plotVolatilityByTime <- function(
    pair,
    timeframe,
    numPeriodsForEstimate = 10L,
    breakpoints = NULL,
    plotTitle = NULL
)
{
    stopifnot(
        is.character(pair), length(pair) == 1L,
        length(timeframe) == 2L, is.POSIXct(timeframe),
        is.numeric(numPeriodsForEstimate), length(numPeriodsForEstimate) == 1L,
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))
    )
    
    # Zeitzone ist UTC, fehlt aber in `timeframe` aus unbekannten Gründen
    setattr(timeframe, "tzone", "UTC")
    
    plotXLab <- "Datum"
    plotYLab <- "Volatilität"
    plotTextPrefix <- "\\footnotesize "
    
    # Volatilität berechnen
    exchanges <- names(exchangeNames)
    result <- data.table()
    for (exchange in exchanges) {
        sourceFile <- sprintf(
            "Cache/%1$s/%2$s/%1$s-%2$s-daily.fst",
            tolower(exchange), tolower(pair)
        )
        
        if (!file.exists(sourceFile)) {
            # Dieses Paar wird an dieser Börse nicht gehandelt
            next
        }
        
        dataset <- read_fst(sourceFile, columns=c("Time", "Mean"), as.data.table=TRUE)
        dataset[, Volatility:=volatility(dataset$Mean, n=numPeriodsForEstimate, N=365)]
        result <- rbindlist(list(result, dataset[Time %between% timeframe & !is.na(Volatility)]))
    }
    
    stopifnot(nrow(result) > 0L)
    
    result <- result[j=.(Volatility=mean(Volatility)), by=Time]
    setorder(result, Time)
    
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
    
    # Volumen zeichnen
    plot <- plot +
        geom_line(aes(x=Time, y=Volatility, color="1", linetype="1"), size=.5) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_x_datetime(
            date_breaks = eval(date_breaks),
            date_labels = date_labels,
            expand = expansion(mult=c(.01, .05))
        ) +
        scale_y_continuous(labels = format.number) +
        # Ähnlich wie scale_color_ptol, aber mit höherem Kontrast
        scale_color_highcontrast() +
        # Kompromiss aus guter Erkennbarkeit bei SW + Farbe als Hintergrund
        scale_fill_pale() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
    }
    
    return(plot)
}


#' Zeichne Preisabweichungen als Boxplot nach Börsenpaar gruppiert
#' 
#' @param comparablePrices `data.table` mit den Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @return Der Plot (unsichtbar)
plotPriceDifferencesBoxplotByExchangePair <- function(
    comparablePrices,
    latexOutPath = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(comparablePrices), nrow(comparablePrices) > 0L,
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))
    )
    
    # Achsenbeschriftung
    plotXLab <- "Börsenpaar"
    plotYLab <- "Preisabweichung"
    plotTextPrefix <- "\\footnotesize "
    
    # Zeichnen
    if (!is.null(comparablePrices$Q3)) {
        maxValue <- max(comparablePrices$Q3)
    } else if (!is.null(comparablePrices$PriceDifference)) {
        maxValue <- max(comparablePrices$PriceDifference)
    } else {
        stop("Keine Quartile und keine Rohdaten gefunden!")
    }
    
    
    # Werte berechnen: Wesentlich schneller,
    # als geom_boxplot die Berechnung übernehmen zu lassen.
    # Das grafische Ergebnis ist identisch, da ohnehin 
    # keine Ausreißer angezeigt werden.
    boxplot_stats <- data.table()
    exchange_pairs <- c()
    for (i in seq_along(exchangeNames)) {
        exchange_1 <- names(exchangeNames)[[i]]
        for (j in seq_along(exchangeNames)) {
            if (j <= i) {
                next
            }
            exchange_2 <- names(exchangeNames)[[j]]
            pairSummary <- summary(
                comparablePrices[
                    (
                        (ExchangeHigh == exchange_1 & ExchangeLow == exchange_2) |
                        (ExchangeHigh == exchange_2 & ExchangeLow == exchange_1)
                    ),
                    PriceDifference
                ]
            )
            if (is.na(pairSummary[[1]])) {
                next
            }
            
            # Namen zusammenfügen und für spätere ggplot-Levels speichern
            pairName <- sprintf(
                "%s,\n%s", exchangeNames[[i]], exchangeNames[[j]]
            )
            exchange_pairs <- c(exchange_pairs, pairName)
            
            iqr <- pairSummary[[5]] - pairSummary[[2]]
            boxplot_stats <- rbindlist(list(boxplot_stats, data.table(
                ExchangePair = pairName,
                ColorGroup = "Börsenpaar",
                min = max(pairSummary[[1]], pairSummary[[2]] - 1.5*iqr),
                lower = pairSummary[[2]],
                middle = pairSummary[[3]],
                upper = pairSummary[[5]],
                max = min(pairSummary[[6]], pairSummary[[5]] + 1.5*iqr)
            )))
        }
    }
    
    # Globale Statistiken nur dann, wenn mehr als ein Börsenpaar vorhanden ist
    if (nrow(boxplot_stats) > 1L) {
        totalSummary <- summary(comparablePrices$PriceDifference)
        iqr <- totalSummary[[5]] - totalSummary[[2]]
        boxplot_stats <- rbindlist(list(boxplot_stats, data.table(
            ExchangePair = "Gesamt",
            ColorGroup = "Gesamt",
            min = max(totalSummary[[1]], totalSummary[[2]] - 1.5*iqr),
            lower = totalSummary[[2]],
            middle = totalSummary[[3]],
            upper = totalSummary[[5]],
            max = min(totalSummary[[6]], totalSummary[[5]] + 1.5*iqr)
        )))
        boxplot_stats[,ExchangePair:=factor(
            ExchangePair,
            levels=c("Gesamt", exchange_pairs)
        )]
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
            aes(x = ExchangePair,
                group = ExchangePair,
                color = ColorGroup,
                ymin = min,
                ymax = max
            ),
            width = .75,
            position = position_dodge(width = 0.9)
        ) +
        
        # Boxplot
        geom_boxplot(
            aes(
                x = ExchangePair,
                group = ExchangePair,
                color = ColorGroup,
                ymin = min,
                lower = lower,
                middle = middle,
                upper = upper,
                ymax = max
            ),
            stat = "identity",
            width = .75
        ) +
        scale_y_continuous(
            labels = function(x) paste0(format.number(x * 100), "\\,%"),
            limits = c(0, max(boxplot_stats$max)*1.0005)
        ) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
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
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param plotTitle Optionaler Plot-Titel
#' @return Der Plot (unsichtbar)
plotDistribution <- function(
    priceDifferences,
    latexOutPath = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences), nrow(priceDifferences) > 0L,
        !is.null(priceDifferences$PriceDifference),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))
    )
    
    # Berechnung der Verteilung
    floorToNumberOfDigits <- 4L
    cutoffThreshold <- 0.02
    
    distribution <- priceDifferences[
        # Nur Preisunterschiede unterhalb des Grenzwertes anzeigen
        i = PriceDifference <= cutoffThreshold,
        # Häufigkeit zählen
        j = .(n = .N),
        # Gruppieren nach dem gerundeten Wert
        by = .(
            PriceDifference = floor(
                PriceDifference * 10^floorToNumberOfDigits
            ) / 10^floorToNumberOfDigits
        )
    ]
    
    # Sortierung nur optional für schönere Darstellung im Terminal etc.
    #setorder(distribution, PriceDifference)
    
    #      PriceDifference        n
    # 1:             0.000 53159730
    # 2:             0.001 43115724
    # ---
    # 687:           1.960        1
    # 688:           9.319        1
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Preisabweichung"
    plotYLab <- "Beobachtungen"
    plotTextPrefix <- "\\footnotesize "
    maxValue <- max(distribution$n)
    
    # Achseneigenschaften
    if (maxValue > 1e6) {
        roundedTo <- "Mio."
        roundFac <- 1e6
    } else {
        roundedTo <- "Tsd."
        roundFac <- 1e3
    }
    
    # Histogramm zeichnen
    barWidth <- 0.75 # Standardwert: 90% = 0.9
    plot <- ggplot(distribution) +
        geom_col(
            aes(x = PriceDifference, y = n, fill = "Häufigkeit"),
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
            breaks = seq(from=0, to=0.02, by=0.002),
            expand = expansion(mult=c(.01, .03))
        ) +
        scale_y_continuous(labels=function(x) format.number(x / roundFac)) +
        scale_color_ptol() +
        scale_fill_ptol() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, " [", roundedTo, "]")
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


#' Zeichne Anteil positiver/negativer Preisabweichungen je Börse
#' 
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param plotTitle Optionaler Plot-Titel
#' @return Der Plot (unsichtbar)
plotPercentageHighLowByExchange <- function(
    priceDifferences,
    latexOutPath = NULL,
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences), nrow(priceDifferences) > 0L,
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(plotTitle) || (length(plotTitle) == 1L && is.character(plotTitle))
    )
    
    # Ergebnis berechnen
    result <- data.table()
    
    # 1. Anzahl gesamter Tauschpaare zählen
    for (i in seq_along(exchangeNames)) {
        exchange <- names(exchangeNames)[[i]]
        result <- rbindlist(list(result, list(
            exchange = exchange,
            exchangeName = exchangeNames[[i]],
            n = priceDifferences[ExchangeHigh == exchange, .N] +
                priceDifferences[ExchangeLow == exchange, .N]
        )))
    }
    
    # 2. Absolute Anzahl Höchst-/Tiefstkurse
    # Hier als Einzelschritte zur besseren Lesbarkeit
    resultHigh <- priceDifferences[j=.(nHigh=.N), by=ExchangeHigh]
    result <- result[resultHigh, on=.(exchange=ExchangeHigh)]
    
    # 3. Anteile berechnen
    result <- result[, `:=`(ratioHigh=nHigh/n, ratioLow=(n-nHigh)/n)]
    result[, `:=`(exchange=NULL, n=NULL, nHigh=NULL)]
    
    # 4. Melt für ggplot
    result <- melt(result, id.vars=c("exchangeName"), variable.name="type", value.name="ratio")
    setorder(result, exchangeName)
    result[, type:=factor(
        type,
        levels = c("ratioHigh", "ratioLow"),
        labels = c("Höchstpreis", "Tiefstpreis")
    )]
    
    # Plot
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Börse"
    plotYLab <- "Anteil"
    plotFillLab <- "Art"
    plotTextPrefix <- "\\footnotesize "
    
    # Histogramm zeichnen
    plot <- ggplot(result) +
        geom_col(aes(x=exchangeName, y=ratio, fill=type), position="dodge", width=.75) +
        geom_text(
            aes(
                x = exchangeName,
                y = ratio + .04,
                group = type,
                label = paste0("\\scriptsize ", format.percentage(ratio, 1L), "\\,\\%")
            ),
            position = position_dodge(width=.75)
        ) +
        theme_minimal() +
        theme(
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
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
        printf("Erzeuge Überblickstabelle in %s...\n", basename(outFile))
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
        
        # Anzahl "vorteilhafter" Preisunterschiede bestimmen
        # > 1 %
        numRowsLargerThan_A <- length(which(dataSubset$PriceDifference >= .01))
        # > 2 %
        numRowsLargerThan_B <- length(which(dataSubset$PriceDifference >= .02))
        # > 5 %
        numRowsLargerThan_C <- length(which(dataSubset$PriceDifference >= .05))
        
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
                format.percentage(median(dataSubset$PriceDifference), 1)
            ),
            
            # Zeilenende
            lineEnd
        ))
    }
    
    tableContent <- ""
    
    # Anzahl Börsenpaare
    exchangePairsInThisSubset <- unique(
        c(unique(dataset$ExchangeHigh), unique(dataset$ExchangeLow))
    )
    
    if (!is.null(forceTablePosition)) {
        tablePosition <- forceTablePosition
    } else if (length(exchangePairsInThisSubset) < 4L) {
        # Tabelle kann im Fließtext unterkommen
        tablePosition <- "tbh"
    } else {
        # Eine ganze Seite für die Statistik reservieren
        tablePosition <- "p"
    }
    
    if (length(exchangePairsInThisSubset) == 2L) {
        
        # Nur ein Börsenpaar: Kompakte Statistiken für gesamten Datensatz
        exchangePairsInThisSubset <- sort(exchangePairsInThisSubset)
        exchange_1 <- exchangePairsInThisSubset[1]
        exchange_2 <- exchangePairsInThisSubset[2]
        tableContent <- paste0(
            tableContent,
            sprintf("        %s mit %s &\n",
                    exchangeNames[[exchange_1]], exchangeNames[[exchange_2]]),
            createRow(numRowsTotal, dataset)
        )
        
    } else {
        
        # Jede Börse durchgehen + Gesamtstatistik erstellen
        vspaceTemplate <- "        \\rule{0pt}{9mm}\n"
        iterationCounter <- 0L
        for (i in seq_along(exchangeNames)) {
            exchange <- names(exchangeNames)[[i]]
            allRows <- dataset[ExchangeHigh == exchange | ExchangeLow == exchange, which=TRUE]
            if (length(allRows) > 0L) {
                iterationCounter <- iterationCounter + 1L
                if (iterationCounter > 1L) {
                    vspace <- vspaceTemplate
                } else {
                    vspace <- ""
                }
                
                # Gesamtüberblick für diese Börse
                tableContent <- paste0(
                    tableContent,
                    "\n",
                    "        \\rowcolor{white}\n",
                    vspace,
                    sprintf("        \\textbf{%s} &\n", exchangeNames[[i]]),
                    createRow(length(allRows), dataset[allRows], lineEnd = "\n"),
                    "        \\global\\rownum=2\\relax\\\\\n\n"
                )
                
                # Paarweise mit anderen Börsen
                for (j in seq_along(exchangeNames)) {
                    if (i == j) {
                        next
                    }
                    otherExchange <- names(exchangeNames)[[j]]
                    rowsWithOtherExchange <- dataset[
                        (ExchangeHigh == exchange & ExchangeLow == otherExchange) |
                        (ExchangeHigh == otherExchange & ExchangeLow == exchange),
                        which = TRUE
                    ]
                    if (length(rowsWithOtherExchange) > 0L) {
                        tableContent <- paste0(
                            tableContent,
                            sprintf("        ~davon mit %s &\n", exchangeNames[[j]]),
                            createRow(length(rowsWithOtherExchange), dataset[rowsWithOtherExchange])
                        )
                    }
                }
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

#' Preisabweichungen grafisch und tabellarisch darstellen
#' 
#' @param pair Das betrachtete Handelspaar, z.B. btcusd
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param threshold Zeitliche Differenz zweier Ticks in Sekunden,
#'                  ab der das Tick-Paar verworfen wird.
#' @param onlyMainGraphAndTable Grafiken und Tabellen für Teil-Intervalle (nicht) erstellen
#' @param appendThresholdToTableLabel Grenzwert an Tabellen-Label anhängen
#' @param overviewImageHeight Höhe der Überblicksgrafik überschreiben
#' @param forceTablePosition Bestimmte Tabellenposition erzwingen
analysePriceDifferences <- function(
    pair,
    breakpoints,
    threshold,
    onlyMainGraphAndTable = FALSE,
    appendThresholdToTableLabel = FALSE,
    overviewImageHeight = NULL,
    forceTablePosition = NULL
)
{
    # Parameter validieren
    stopifnot(
        length(breakpoints) >= 1L,
        is.character(pair), length(pair) == 1L, nchar(pair) == 6L,
        is.numeric(threshold), length(threshold) == 1L,
        is.logical(onlyMainGraphAndTable), length(onlyMainGraphAndTable) == 1L,
        is.logical(appendThresholdToTableLabel), length(appendThresholdToTableLabel) == 1L,
        (is.null(overviewImageHeight) || is.numeric(overviewImageHeight))
    )
    
    # Vorherige Berechnungen ggf. aus dem Speicher bereinigen
    gc()
    
    # Verzeichnisse anlegen
    tableOutPath <- sprintf(
        "%s/Tabellen/Raumarbitrage/%ds/%s",
        latexOutPath, threshold, pair
    )
    plotOutPath <- sprintf(
        "%s/Abbildungen/Raumarbitrage/%ds/%s",
        latexOutPath, threshold, pair
    )
    for (d in c(tableOutPath, plotOutPath)) {
        if (!dir.exists(d)) {
            dir.create(d, recursive=TRUE)
        }
    }
    
    # Daten laden
    comparablePrices <- loadComparablePricesByCurrencyPair(pair, threshold)
    
    # Monatsdaten berechnen
    aggregatedPriceDifferences <- aggregatePriceDifferences(comparablePrices, "1 month")

    # Überblicksgrafik (Ganze Seite) erstellen
    p_diff <- plotAggregatedPriceDifferencesByTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        # Lücken werden immer auf 1d-Basis entfernt.
        # Da es sich um Monatsdaten handelt, wäre der Plot leer...
        removeGaps = FALSE,
        plotTitle = "Preisabweichungen"
    )
    p_profitable <- plotProfitableDifferencesByTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        plotTitle = "Arbitrage-Paare mit einer Abweichung von min. 1\\,%, 2\\,% und 5\\,%"
    )
    p_nrow <- plotNumDifferencesByTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        plotTitle = "Anzahl Beobachtungen"
    )
    p_vola <- plotVolatilityByTime(
        pair,
        aggregatedPriceDifferences[c(1,.N), Time],
        breakpoints = breakpoints,
        plotTitle = "Rollierende, annualisierte 10-Tage-Volatilität der Preise"
    )

    # Als LaTeX-Dokument ausgeben
    if (is.null(overviewImageHeight)) {
        overviewImageHeight <- 22L
    }

    source("Konfiguration/TikZ.R")
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
            pair,
            aggregatedPriceDifferences[c(1,.N), Time],
            breakpoints = breakpoints,
            plotTitle = "Preisniveau"
        )
        p_volume <- plotTradingVolumeByTime(
            pair,
            aggregatedPriceDifferences[c(1,.N), Time],
            breakpoints = breakpoints,
            plotTitle = "Handelsvolumen"
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
        plotPriceDifferencesBoxplotByExchangePair(
            comparablePrices,
            latexOutPath = sprintf("%s/Boxplot_Gesamt.tex", plotOutPath)
        )
        
        # Verteilungs-Histogramm
        plotDistribution(
            comparablePrices,
            latexOutPath = sprintf("%s/Histogramm_Gesamt.tex", plotOutPath)
        )
        
        # Anteil Höchst-/Tiefstpreise nach Börse
        plotPercentageHighLowByExchange(
            comparablePrices,
            latexOutPath = sprintf("%s/Anteile_Hoechst_Tiefst_nach_Boerse_Gesamt.tex", plotOutPath)
        )
    }
    
    # Speicher freigeben
    rm(aggregatedPriceDifferences)
    gc()
    
    
    # Überblickstabelle
    if (appendThresholdToTableLabel) {
        tableLabelAppendix <- sprintf(" (Grenzwert %ds)", threshold)
    } else {
        tableLabelAppendix <- ""
    }
    summariseDatasetAsTable(
        comparablePrices,
        outFile = sprintf("%s/Uebersicht_Gesamt.tex", tableOutPath),
        caption = sprintf(
            "Kenngrößen der Preisabweichungen von %s%s",
            format.currencyPair(pair), tableLabelAppendix
        ),
        label = sprintf("Raumarbitrage_%s_%ds_Uebersicht_Gesamt", toupper(pair), threshold),
        forceTablePosition = forceTablePosition
    )
    
    # Keine weitere Auswertung
    if (onlyMainGraphAndTable) {
        return(invisible(NULL))
    }
    
    # Intervalle bestimmen
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    for (segment in seq_len(nrow(intervals))) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        comparablePricesSubset <- comparablePrices[Time %between% segmentInterval]
        
        printf(
            "Intervall #%d: %s Datensätze von %s bis %s.\n",
            segment,
            format.number(nrow(comparablePricesSubset)),
            format(segmentInterval[1], "%d.%m.%Y"),
            format(segmentInterval[2], "%d.%m.%Y")
        )
        
        # Daten auf eine Woche aggregieren (besser lesbar mit größeren Intervallen)
        aggregatedPriceDifferences <- aggregatePriceDifferences(
            comparablePricesSubset,
            floorUnits = "1 week"
        )

        if (nrow(aggregatedPriceDifferences) > 50L) {

            # Variante 1: Aggregierte Liniengrafik: Nur sinnvoll, wenn keine/wenige Lücken
            p_diff <- plotAggregatedPriceDifferencesByTime(
                aggregatedPriceDifferences,
                plotTitle = "Preisabweichungen",
                # Lücken nicht entfernen, da 1w-Daten, keine Tagesdaten
                removeGaps = FALSE,
                #latexOutPath = sprintf("%s/Preisabweichungen_%d.tex", plotOutPath, segment)
            )

        } else {

            # Variante 2: Punktgrafik: Sinnvoll auch bei vielen Lücken, nicht aber
            # bei großen Datenmengen. Zeichnet *alle* Daten, nicht aggregierte Daten
            p_diff <- plotAggregatedPriceDifferencesByTime(
                comparablePricesSubset,
                plotType = "point",
                plotTitle = "Preisabweichungen"
                #latexOutPath = sprintf("%s/Preisabweichungen_%d.tex", plotOutPath, segment)
            )
            
        }

        # Spezielle Grafikkonfiguration abrufen, falls vorhanden
        legendBelowGraph <- plotConfiguration[["profitableDifferencesByTime"]][[pair]][[as.character(segment)]]
        p_profitable <- plotProfitableDifferencesByTime(
            aggregatedPriceDifferences,
            plotTitle = "Arbitrage-Paare mit einer Abweichung von min. 1\\,%, 2\\,% und 5\\,%",
            legendBelowGraph = legendBelowGraph
        )
        p_nrow <- plotNumDifferencesByTime(
            aggregatedPriceDifferences,
            plotTitle = "Anzahl Beobachtungen"
        )
        p_vola <- plotVolatilityByTime(
            pair,
            aggregatedPriceDifferences[c(1,.N), Time],
            plotTitle = "Rollierende, annualisierte 10-Tage-Volatilität der Preise"
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
        plotPriceDifferencesBoxplotByExchangePair(
            comparablePricesSubset,
            latexOutPath = sprintf("%s/Boxplot_%d.tex", plotOutPath, segment)
        )
        
        # Verteilungs-Histogramm mit Daten des Intervalls
        plotDistribution(
            comparablePricesSubset,
            latexOutPath = sprintf("%s/Histogramm_%d.tex", plotOutPath, segment)
        )
        
        # Statistiken in Tabelle ausgeben
        summariseDatasetAsTable(
            comparablePricesSubset,
            outFile = sprintf("%s/Uebersicht_%d.tex", tableOutPath, segment),
            caption = sprintf(
                "Kenngrößen der Preisabweichungen von %s (%s bis %s)",
                format.currencyPair(pair),
                format(segmentInterval[1], "%B~%Y"),
                format(segmentInterval[2], "%B~%Y")
            ),
            indexCaption = sprintf(
                "Kenngrößen der Preisabweichungen von %s (%s -- %s)",
                format.currencyPair(pair),
                format(segmentInterval[1], "%m/%Y"),
                format(segmentInterval[2], "%m/%Y")
            ),
            label = sprintf(
                "Raumarbitrage_%s_%ds_Uebersicht_%d",
                toupper(pair), threshold, segment
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
    
    # Achtung: Immenser Bedarf an Arbeitsspeicher! (> 20 GB)
    for (threshold in thresholds) {
        for (pair in currencyPairs) {
            printf("\n\n=== Betrachte %s für den Schwellwert %ds... ===\n", pair, threshold)
            
            if (threshold != mainThreshold) {
                overviewImageHeight <- 20L
                forceTablePosition <- "p"
            } else {
                overviewImageHeight <- NULL
                forceTablePosition <- NULL
            }
            
            analysePriceDifferences(
                pair,
                breakpointsByCurrency[[pair]],
                threshold,
                onlyMainGraphAndTable = (threshold != mainThreshold),
                appendThresholdToTableLabel = (threshold != mainThreshold),
                overviewImageHeight = overviewImageHeight,
                forceTablePosition = forceTablePosition
            )
            gc()
        }
    }
}
