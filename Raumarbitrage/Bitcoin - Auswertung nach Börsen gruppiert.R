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


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Funktionen/FormatCurrencyPair.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")
library("fst")
library("data.table")
library("lubridate") # floor_date
library("ggplot2")
library("khroma") # Farbschemata von Paul Tol
library("gridExtra") # grid.arrange
library("readr") # read_file, write_file
library("stringr") # str_replace
library("tictoc")


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
thresholds <- c(1L, 2L, 5L, 10L)

# Haupt-Intervall zur Betrachtung innerhalb der Arbeit
# (Rest: Nur Anhang, reduzierte Ansicht)
mainThreshold <- 1L

# Breakpoints für eine detailliertere Betrachtung einzelner Zeitabschnitte
# Die Breakpoints selbst werden immer dem letzten der beiden entstehenden
# Intervalle zugerechnet
breakpointsByCurrency <- list(
    # Intervall: 1.           2.             3.           4.
    # Von/Bis: -> 2014 -- 2014-2017  --  2017-2019 -- 2019 ->
    "btcusd" = c("2014-03-01", "2017-01-01", "2019-07-01"),
    "btceur" = c(              "2017-01-01", "2019-07-01")
)

#' Tabellen-Template mit `{tableContent}`, `{tableCaption` und `{tableLabel}` 
#' als Platzhalter
summaryTableTemplateFile <- 
    sprintf("%s/Tabellen/Templates/Raumarbitrage_Uebersicht_nach_Boerse.tex",
            latexOutPath)


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
        is.character(currencyPair), length(currencyPair) == 1L,
        nchar(currencyPair) == 6L,
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
    maxDifference <- combinedPriceDifferences[which.max(PriceDifference)]
    with(maxDifference, printf(
        "Höchstwert: %s (%s <-> %s) am %s\n",
        format.number(PriceDifference),
        format.money(PriceLow),
        format.money(PriceHigh),
        formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
    ))
    
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
    floorUnits,
    interval = NULL
) {
    
    # Parameter validieren
    stopifnot(
        is.data.table(comparablePrices), nrow(comparablePrices) > 0L,
        !is.null(comparablePrices$Time), !is.null(comparablePrices$PriceDifference),
        is.character(floorUnits), length(floorUnits) == 1L,
        is.null(interval) || length(interval) == 2L
    )
    
    tic()
    
    # Zeitraum eingrenzen
    if (!is.null(interval)) {
        comparablePrices <- comparablePrices[Time %between% interval]
    }
    
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


#' Intervalle mit den angegebenen Breakpoints berechnen
calculateIntervals <- function(timeBoundaries, breakpoints)
{
    breakpoints <- as.POSIXct(breakpoints)
    intervals <- data.table()
    prevDate <- as.Date(min(timeBoundaries))
    for (i in seq_along(breakpoints)) {
        intervals <- rbindlist(list(intervals, data.table(
            From = c(prevDate),
            To = c(as.Date(breakpoints[i] - 1)),
            Set = c(as.character(i))
        )))
        prevDate <- as.Date(breakpoints[i])
    }
    intervals <- rbindlist(list(intervals, data.table(
        From = c(prevDate),
        To = c(as.Date(max(timeBoundaries))),
        Set = c(as.character(i+1))
    )))
    intervals$From <- as.POSIXct(intervals$From)
    intervals$To <- as.POSIXct(intervals$To)
    
    return(intervals)
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
plotAggregatedPriceDifferencesOverTime <- function(
    priceDifferences,
    latexOutPath = NULL,
    breakpoints = NULL,
    removeGaps = TRUE,
    plotType = "line",
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences), nrow(priceDifferences) > 0L,
        !is.null(priceDifferences$Time),
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
    
    
    plot <- plot + 
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
        coord_cartesian(ylim=c(0, maxValue)) +
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
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param timeHorizon Aggregationsebene (für Beschriftung der Y-Achse)
#' @param plotTitle Überschrift (optional)
#' @return Der Plot (unsichtbar)
plotNumDifferencesOverTime <- function(
    priceDifferences,
    breakpoints = NULL,
    timeHorizon = "Monatliche",
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences), nrow(priceDifferences) > 0L,
        !is.null(priceDifferences$Time)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- paste0(timeHorizon, " Beobachtungen")
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
    
    # Anzahl Datensätze zeichnen
    plot <- plot +
        geom_line(
            aes(x=Time, y=n, color="1", linetype="1"),
            size = .5
        )  + 
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title.position = "plot",
            axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
            axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
        ) +
        scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
        coord_cartesian(ylim=c(0, maxValue)) +
        scale_y_continuous(
            labels = function(x) paste(format.number(x / roundFac))
            #breaks = breaks_extended(4L)
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
#' @return Der Plot (unsichtbar)
plotProfitableDifferencesOverTime <- function(
    priceDifferences,
    latexOutPath = NULL,
    breakpoints = NULL,
    plotTitle = NULL
)
{
    # Parameter validieren
    stopifnot(
        is.data.table(priceDifferences),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- "Anteil"
    plotTextPrefix <- "\\footnotesize "
    
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
    
    # 1. Gesamtübersicht, gruppiert nach Grenzwerten, aggregiert
    plot <- ggplot(priceDifferences)
    
    # Maximalwert entsteht immer bei der geringsten Abweichung
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
            legend.position = c(0.88, 0.58),
            legend.background = element_rect(fill="white", size=0.2, linetype="solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(),
        ) +
        scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
        coord_cartesian(ylim=c(0, maxValue)) +
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
    
    if (!is.null(latexOutPath)) {
        # Plot zeichnen
        print(plot)
        dev.off()
        return(invisible(plot))
    } else {
        return(plot)
    }
    
    # 2. Übersicht nach Börsen, gruppiert nach Grenzwerten
    # TODO Schwierig, müsste nochmals nach Börsenpaar aggregiert werden!
    
}


#' Zeichne das gehandelte Volumen eines Kurspaares an allen Börsen für den
#' angegebenen Zeitabschnitt.
#' 
#' @param pair Das gewünschte Kurspaar, bspw. btcusd
#' @param timeframe POSIXct-Vektor der Länge 2 mit den Grenzen des Intervalls (inkl.)
#' @return Plot
plotTotalVolumeOverTime <- function(
    pair,
    timeframe,
    breakpoints = NULL,
    plotTitle = NULL
)
{
    stopifnot(
        is.character(pair), length(pair) == 1L,
        length(timeframe) == 2L, is.POSIXct(timeframe)
        # TODO Breakpoints (auch oben)
    )
    
    plotXLab = "Datum"
    plotYLab = "Volumen"
    plotTextPrefix <- "\\footnotesize "
    
    # Handelsvolumen berechnen
    exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")
    result <- data.table()
    for (exchange in exchanges) {
        sourceFile <- sprintf("Cache/%1$s/%2$s/%1$s-%2$s-monthly.fst", 
                              tolower(exchange), tolower(pair))
        
        if (!file.exists(sourceFile)) {
            # Dieses Paar wird an dieser Börse nicht gehandelt
            next
        }
        
        dataset <- read_fst(
            sourceFile, 
            columns = c("Time", "Amount"), 
            as.data.table = TRUE
        )
        dataset$Time <- as.POSIXct(dataset$Time, tz="UTC")
        result <- rbindlist(list(result, dataset[Time %between% timeframe]))
    }
    
    result <- result[j=.(Amount=sum(Amount)), by=Time]
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
        scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
        coord_cartesian(ylim=c(0, maxValue)) +
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
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L)
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
        
        # TODO Zurück zu _ptol() aus ggthemes?
        scale_color_highcontrast() +
        scale_fill_highcontrast() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0("\\small ", plotTitle))
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


#' Informationen über einen Datensatz als LaTeX-Tabelle ausgeben
#' 
#' @param dataset `data.table` aus `loadComparablePricesByCurrency`
#'                             oder aus `aggregatePriceDifferences`
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
    printf("Erzeuge Überblickstabelle in %s ", basename(outFile))
    numRowsTotal <- nrow(dataset)
    
    # Tabellenzeile erzeugen
    createRow <- function(numRows, dataSubset, end="\\\\\n\n") {
        printf(".")
        intervalLengthHours <- 
            difftime(last(dataSubset$Time), first(dataSubset$Time), units = "hours") |>
            round() |>
            as.double()
        numRowsPerHour <- numRows / intervalLengthHours
        numRowsPerDay <- numRows / (intervalLengthHours / 24)
        
        # Anzahl "vorteilhafter" Preisunterschiede bestimmen
        # > 1 %
        numRowsLargerThan_A <- length(which(dataSubset$PriceDifference >= .01))
        # > 2 %
        numRowsLargerThan_B <- length(which(dataSubset$PriceDifference >= .02))
        # > 5 %
        numRowsLargerThan_C <- length(which(dataSubset$PriceDifference >= .05))
        s <- strrep(" ", 12) # Einrückung in der Ergebnisdatei
        return(paste0(
            sprintf("%s%% %s Datensätze pro Tag\n", s,
                    format.numberWithFixedDigits(numRowsPerDay, digits=1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRows),
                    format.percentage(numRows / numRowsTotal, 1L)),
            sprintf("%s%s &\n", s, format.numberWithFixedDigits(numRowsPerHour, 1)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan_A),
                    format.percentage(numRowsLargerThan_A / numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan_B),
                    format.percentage(numRowsLargerThan_B / numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan_C),
                    format.percentage(numRowsLargerThan_C / numRows, 1L)),
            sprintf("%s%s\\,\\%% ", s, 
                    format.percentage(max(dataSubset$PriceDifference), 1)),
            end
        ))
    }
    
    tableContent <- ""
    
    # Anzahl Börsenpaare
    exchangePairsInThisSubset <- unique(
        c(
            unique(dataset$ExchangeHigh),
            unique(dataset$ExchangeLow)
        )
    )
    
    
    if (length(exchangePairsInThisSubset) < 4L) {
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
                    createRow(length(allRows), dataset[allRows], end = "\n"),
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

#' Preisabweichungen grafisch und tabellarisch darstellen
#' 
#' @param pair Das betrachtete Handelspaar, z.B. btcusd
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param threshold Zeitliche Differenz zweier Ticks in Sekunden,
#'                  ab der das Tick-Paar verworfen wird.
#' @param plotTradingVolume Plot des Handelsvolumens erzeugen
#' @param analysePartialIntervals Grafiken und Tabellen für Teil-Intervalle erstellen
#' @param appendThresholdToTableLabel Grenzwert an Tabellen-Label anhängen
analysePriceDifferences <- function(
    pair,
    breakpoints,
    threshold,
    plotTradingVolume = TRUE,
    analysePartialIntervals = TRUE,
    appendThresholdToTableLabel = FALSE
)
{
    # Parameter validieren
    stopifnot(
        length(breakpoints) >= 1L,
        is.character(pair), length(pair) == 1L, nchar(pair) == 6L,
        is.numeric(threshold), length(threshold) == 1L,
        is.logical(plotTradingVolume), length(plotTradingVolume) == 1L,
        is.logical(analysePartialIntervals), length(analysePartialIntervals) == 1L,
        is.logical(appendThresholdToTableLabel), length(appendThresholdToTableLabel) == 1L
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
    p_diff <- plotAggregatedPriceDifferencesOverTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        # Lücken werden immer auf 1d-Basis entfernt.
        # Da es sich um Monatsdaten handelt, wäre der Plot leer...
        removeGaps = FALSE,
        plotTitle = "Preisabweichungen"
    )
    p_profitable <- plotProfitableDifferencesOverTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        plotTitle = "Anteil der Arbitrage-Paare mit einer Abweichung von min. 1\\,%, 2\\,% und 5\\,%"
    )
    p_nrow <- plotNumDifferencesOverTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        timeHorizon = "",
        plotTitle = "Anzahl monatlicher Beobachtungen"
    )
    if (plotTradingVolume) {
        p_volume <- plotTotalVolumeOverTime(
            pair,
            aggregatedPriceDifferences$Time[c(1,nrow(aggregatedPriceDifferences))],
            breakpoints = breakpoints,
            plotTitle = "Handelsvolumen"
        )
    }
    
    # Als LaTeX-Dokument ausgeben
    source("Konfiguration/TikZ.R")
    if (plotTradingVolume) {
        tikz(
            file = sprintf("%s/Uebersicht.tex", plotOutPath),
            width = documentPageWidth,
            height = 22 / 2.54,
            sanitize = TRUE
        )
        grid.arrange(
            p_diff, p_profitable, p_nrow, p_volume,
            layout_matrix = rbind(c(1),c(2),c(3),c(4))
        )
    } else {
        tikz(
            file = sprintf("%s/Uebersicht.tex", plotOutPath),
            width = documentPageWidth,
            height = 16.5 / 2.54,
            sanitize = TRUE
        )
        grid.arrange(
            p_diff, p_profitable, p_nrow,
            layout_matrix = rbind(c(1),c(2),c(3))
        )
    }
    dev.off()
    
    # Boxplot
    plotPriceDifferencesBoxplotByExchangePair(
        comparablePrices,
        latexOutPath = sprintf("%s/Uebersicht_Boxplot.tex", plotOutPath)
        #plotTitle = "Lagemaße der Preisabweichungen nach Börsenpaar"
    )
    
    # Speicherdruck reduzieren
    rm(aggregatedPriceDifferences)
    gc()
    
    # Beschreibende Statistiken
    if (appendThresholdToTableLabel) {
        tableLabelAppendix <- sprintf(" (Grenzwert %ds)", threshold)
    } else {
        tableLabelAppendix <- ""
    }
    summariseDatasetAsTable(
        comparablePrices,
        outFile = sprintf("%s/Uebersicht.tex", tableOutPath),
        caption = sprintf(
            "Zentrale Kenngrößen paarweiser Preisnotierungen für %s im Gesamtüberblick%s",
            format.currencyPair(pair), tableLabelAppendix
        ),
        label = sprintf("Raumarbitrage_%s_Ueberblick_%ds", toupper(pair), threshold)
    )
    
    # Einzelne Segmente auswerten
    if (!analysePartialIntervals) {
        return(invisible(NULL))
    }
    
    # Intervalle bestimmen
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    for (segment in seq_len(nrow(intervals))) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        
        printf(
            "%s Datenpunkte im Intervall von %s bis %s.\n",
            format.number(nrow(comparablePrices[Time %between% segmentInterval])),
            format(segmentInterval[1], "%d.%m.%Y"),
            format(segmentInterval[2], "%d.%m.%Y")
        )
        
        # Daten auf einen Tag aggregieren
        aggregatedPriceDifferences <- aggregatePriceDifferences(
            comparablePrices,
            floorUnits = "1 day",
            interval = segmentInterval
        )
        
        if (nrow(aggregatedPriceDifferences) > 50L) {
            
            # Variante 1: Aggregierte Liniengrafik: Nur sinnvoll, wenn keine/wenige Lücken
            plotAggregatedPriceDifferencesOverTime(
                aggregatedPriceDifferences,
                latexOutPath = sprintf("%s/Abschnitt_%d.tex", plotOutPath, segment)
            )
            
        } else {
            
            # Variante 2: Punktgrafik: Sinnvoll auch bei vielen Lücken, nicht aber
            # bei großen Datenmengen. Zeichnet *alle* Daten, nicht aggregierte Daten
            plotAggregatedPriceDifferencesOverTime(
                comparablePrices[Time %between% segmentInterval],
                plotType = "point",
                latexOutPath = sprintf("%s/Abschnitt_%d.tex", plotOutPath, segment)
            )
            
        }
        
        # Statistiken in Tabelle ausgeben
        summariseDatasetAsTable(
            comparablePrices[Time %between% segmentInterval],
            outFile = sprintf("%s/Abschnitt_%d.tex", tableOutPath, segment),
            caption = sprintf(
                "Zentrale Kenngrößen der Preisabweichungen für %s von %s bis %s",
                format.currencyPair(pair),
                format(segmentInterval[1], "%d.%m.%Y"),
                format(segmentInterval[2], "%d.%m.%Y")
            ),
            label = sprintf(
                "Raumarbitrage_%s_Ueberblick_%ds_%d",
                toupper(pair), threshold, segment
            )
        )
        
        # Boxplot mit Daten des Intervalls
        plotPriceDifferencesBoxplotByExchangePair(
            comparablePrices[Time %between% segmentInterval],
            latexOutPath = sprintf("%s/Boxplot_%d.tex", plotOutPath, segment)
        )
    }
    
    return(invisible(NULL))
}


# Auswertung (grafisch, numerisch) --------------------------------------------

# Händisch einzeln bei Bedarf starten, da große Datenmengen geladen werden
# und die Verarbeitung viel Zeit in Anspruch nimmt.
if (FALSE) {
    
    # Achtung: Immenser Bedarf an Arbeitsspeicher!!! (> 20 GB)
    for (threshold in thresholds) {
        for (pair in currencyPairs) {
            printf("\n\nBetrachte %s für den Schwellwert %ds...\n", pair, threshold)
            analysePriceDifferences(
                pair,
                breakpointsByCurrency[[pair]],
                threshold,
                plotTradingVolume = TRUE,
                analysePartialIntervals = (threshold == mainThreshold),
                appendThresholdToTableLabel = (threshold != mainThreshold)
            )
            gc()
        }
    }
}
