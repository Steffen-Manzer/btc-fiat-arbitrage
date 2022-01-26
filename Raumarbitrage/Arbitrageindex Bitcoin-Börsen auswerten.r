#' Auswertung der Preisunterschiede von Bitcoin-Börsen mit Blick auf
#' Möglichkeiten der Raumarbitrage.
#'
#' Notwendig ist die vorherige paarweise Berechnung und Speicherung relevanter
#' Preisunterschiede unter
#'   `Cache/Raumarbitrage/{Kurspaar}-{Börse 1}-{Börse 2}-{i}` mit `i = 1 ... n`.
#' über die Datei `Preisunterschiede Bitcoin-Börsen berechnen.r`.
#'
#' Stand Januar 2022 passen sämtliche Ergebnisse noch mit etwas Puffer in eine
#' einzelne Ergebnisdatei, ohne dass diese jeweils mehr als 4 GB Arbeitsspeicher
#' belegen würde.
#' 
#' Größte Datei: Bitfinex - Coinbase Pro für BTC/USD
#'   mit 63.107.943 Datensätzen in 2 GB (unkomprimiert).
#'
#' Diese Auswertung lädt aus diesem Grund derzeit je Kurs-/Börsenpaar nur die
#' erste Ergebnisdatei und prüft nicht, ob weitere Ergebnisse vorliegen.
#' Das Nachladen weiterer Ergebnisdateien müsste in Zukunft ergänzt werden, wenn
#' einzelne Dateien die Schwelle von 100 Mio. Datensätzen überschreiten.


# Konfiguration ---------------------------------------------------------------
plotAsLaTeX <- FALSE
DEBUG_PRINT <- TRUE


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/printf.r")
library("fst")
library("data.table")
library("lubridate") # floor_date
library("ggplot2")
library("ggthemes")
library("tictoc")


# Hilfsfunktionen -------------------------------------------------------------

#' Lade alle vergleichbaren Preise für ein Kurspaar
#' 
#' @param currencyPair Kurspaar (z.B. BTCUSD)
#' @return `data.table` mit den Preisunterschieden
loadComparablePricesByCurrencyPair <- function(currencyPair) {
    # Parameter validieren
    stopifnot(
        is.character(currencyPair), length(currencyPair) == 1L,
        nchar(currencyPair) == 6L
    )
    
    # Nur jeweils erste Datei einlesen, siehe oben.
    sourceFiles <- list.files(
        "Cache/Raumarbitrage",
        pattern=sprintf("^%s-.*-1\\.fst$", currencyPair),
        full.names = TRUE
    )
    
    # Variablen initialisieren / leeren
    combinedPriceDifferences <- NULL
    
    # Alle Datensätze einlesen
    for (i in seq_along(sourceFiles)) {
        sourceFile <- sourceFiles[[i]]
        priceDifferences <- read_fst(sourceFile, as.data.table=TRUE)
        priceDifferences[,ExchangePair:=basename(sourceFile)]
        
        numRows <- nrow(priceDifferences)
        
        # Statistiken ausgeben
        if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
            printf.debug(
                "%s (%s) mit %s Datensätzen von %s bis %s.\n", 
                basename(sourceFile),
                format(object.size(priceDifferences), units="auto", standard="SI"),
                format.number(numRows),
                format(first(priceDifferences$Time), "%d.%m.%Y"),
                format(last(priceDifferences$Time), "%d.%m.%Y")
            )
            maxIndexValue <- priceDifferences[which.max(ArbitrageIndex)]
            with(maxIndexValue, printf.debug(
                "Höchstwert: %s (%s <-> %s) am %s\n",
                format.number(ArbitrageIndex),
                format.money(PriceLow),
                format.money(PriceHigh),
                format(Time, "%d.%m.%Y %H:%M:%OS")
            ))
        }
        
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
    
    # Statistiken ausgeben
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        printf.debug(
            "\nKombination aller Börsen ergab %s Datensätze (%s) von %s bis %s.\n",
            format.number(nrow(combinedPriceDifferences)),
            format(object.size(combinedPriceDifferences), units="auto", standard="SI"),
            format(first(combinedPriceDifferences$Time), "%d.%m.%Y"),
            format(last(combinedPriceDifferences$Time), "%d.%m.%Y")
        )
        maxIndexValue <- combinedPriceDifferences[which.max(ArbitrageIndex)]
        with(maxIndexValue, printf.debug(
            "Höchstwert: %s (%s <-> %s) am %s\n",
            format.number(ArbitrageIndex),
            format.money(PriceLow),
            format.money(PriceHigh),
            format(Time, "%d.%m.%Y %H:%M:%OS")
        ))
    }
    
    return(combinedPriceDifferences)
}


#' Aggregiere Arbitrageindex auf den angegebenen Zeitraum
#' 
#' @param comparablePrices `data.table` mit den Preisen der verschiedenen Börsen
#' @param floorUnits Aggregations-Zeitfenster, genutzt als `unit` für
#'   `floor_date`. Werte kleiner als ein Tag sind grafisch kaum darstellbar.
#' @return `data.table` mit Q1, Median, Mean, Q3, Max
aggregateArbitrageIndex <- function(
    comparablePrices,
    floorUnits,
    interval = NULL
) {
    
    # Parameter validieren
    stopifnot(
        is.data.table(comparablePrices), nrow(comparablePrices) > 0L,
        !is.null(comparablePrices$Time), !is.null(comparablePrices$ArbitrageIndex),
        is.character(floorUnits), length(floorUnits) == 1L,
        is.null(interval) || length(interval) == 2L
    )
    
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        tic()
    }
    
    # Zeitraum eingrenzen
    if (!is.null(interval)) {
        comparablePrices <- comparablePrices[Time %between% interval]
    }
    
    # Aggregation aller gefundener Tauschmöglichkeiten
    arbitrageIndex <- comparablePrices[
        j=.(
            Q1 = quantile(ArbitrageIndex, probs=.25, names=FALSE),
            Mean = mean(ArbitrageIndex),
            Median = median(ArbitrageIndex),
            Q3 = quantile(ArbitrageIndex, probs=.75, names=FALSE),
            Max = max(ArbitrageIndex)
        ),
        by=.(Time=floor_date(Time, unit=floorUnits))
    ]
    printf.debug("Aggregation auf '%s' ergab %s Datensätze. ",
                 floorUnits, format.number(nrow(arbitrageIndex)))
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        toc()
    }
    
    return(arbitrageIndex)
}


#' Intervalle berechnen
calculateIntervals <- function(timeBoundaries, breakpoints)
{
    intervals <- data.table()
    prevDate <- as.Date(min(timeBoundaries))
    for (i in seq_along(breakpoints)) {
        intervals <- rbindlist(list(intervals, data.table(
            From = c(prevDate),
            To = c(as.Date(breakpoints[i])),
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


#' Zeichne Arbitrageindex
#' 
#' @param arbitrageIndex `data.table` mit den Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @return `NULL`
plotArbitrageIndex <- function(
    arbitrageIndex,
    latexOutPath = NULL,
    breakpoints = NULL,
    plotType = "lines"
) {
    # Parameter validieren
    stopifnot(
        is.data.table(arbitrageIndex), nrow(arbitrageIndex) > 0L,
        !is.null(arbitrageIndex$Time),
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L),
        is.null(breakpoints) || (is.vector(breakpoints) && length(breakpoints) > 0L)
    )
    
    # Ausgabeoptionen
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.r")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", latexOutPath)
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        
        plotXLab <- "\\footnotesize Datum"
        plotYLab <- "\\footnotesize Arbitrageindex"
        setLabelPrefix <- "\\footnotesize "
    } else {
        plotXLab <- "Datum"
        plotYLab <- "Arbitrageindex"
        setLabelPrefix <- ""
    }
    
    # Zeichnen
    if (!is.null(arbitrageIndex$Q3)) {
        maxValue <- max(arbitrageIndex$Q3)
    } else if (!is.null(arbitrageIndex$ArbitrageIndex)) {
        maxValue <- max(arbitrageIndex$ArbitrageIndex)
    } else {
        stop("Keine Quartile und keine Rohdaten gefunden!")
    }
    plot <- ggplot(arbitrageIndex)
    
    # Bereiche zeichnen und Nummer anzeigen
    if (!is.null(breakpoints)) {
        
        # Die hier bestimmten Intervalle der aggregierten Daten können
        # von den Intervallen des gesamten Datensatzes abweichen
        intervals <- calculateIntervals(arbitrageIndex$Time, breakpoints)
        
        # Grafik um farbige Hintergründe der jeweiligen Segmente ergänzen
        plot <- plot + 
            geom_rect(
                aes(
                    xmin = From,
                    xmax = To,
                    ymin = 1,
                    ymax = maxValue * 1.02,
                    fill = Set
                ),
                data = intervals,
                alpha = .25
            ) +
            geom_text(
                aes(
                    x = From+(To-From)/2,
                    y = maxValue,
                    label = paste0(setLabelPrefix, Set)
                ),
                data = intervals
            )
    }
    
    if (plotType == "lines") {
        
        # Liniengrafik. Nützlich, wenn keine Lücken in den Daten vorhanden sind
        plot <- plot +
            # Q1/Q3 zeichnen
            geom_ribbon(aes(x=Time, ymin=Q1, ymax=Q3), fill="grey70") +
            
            # Median zeichnen
            geom_line(aes(x=Time, y=Median, color="1", linetype="1"), size=.5)
        
    } else if (plotType == "dots") {
        
        # Punkt-Grafik. Besser, wenn Daten viele Lücken aufweisen
        plot <- plot +
            geom_point(aes(x=Time, y=ArbitrageIndex, color="1"), size=.25)
        
    } else {
        stop(sprintf("Unbekannter Plot-Typ: %s", plotType))
    }
        
    plot <- plot +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            #date_breaks = "1 year",
            #date_minor_breaks = "3 months",
            #date_labels = "%Y",
            expand = expansion(mult=c(.01, .03))
        ) +
        scale_y_continuous(
            labels = function(x) paste(format.number(x * 100), "%")#,
            #breaks = seq(from=1, to=ceiling(maxValue*10)/10, by=.1)
        ) +
        coord_cartesian(ylim=c(1, maxValue)) +
        scale_color_ptol() +
        scale_fill_ptol() + 
        labs(x=plotXLab, y=plotYLab)
    
    print(plot)
    
    if (!is.null(latexOutPath)) {
        dev.off()
    }
    
    return(invisible(NULL))
}


# Auswertung (grafisch, numerisch) --------------------------------------------

# Händisch einzeln bei Bedarf starten, da große Datenmengen geladen werden
# und die Verarbeitung viel Zeit in Anspruch nimmt.
if (FALSE) {
    
    # BTC/USD -----------------------------------------------------------------
    
    # Daten laden (~8 GB unkomprimiert)
    comparablePrices <- loadComparablePricesByCurrencyPair("btcusd")
    
    # Intervalle definieren
    breakpoints <- c("2014-03-01", "2017-01-01", "2018-06-01", "2019-07-01", "2020-05-01")
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    
    # Übersicht mit allen Segmenten anzeigen
    arbitrageIndex <- aggregateArbitrageIndex(comparablePrices, "1 month")
    intervals <- calculateIntervals(arbitrageIndex$Time, breakpoints)
    plotArbitrageIndex(arbitrageIndex, breakpoints=breakpoints)
    
    # Beschreibende Statistiken
    # ...
    # TODO
    # - Boxplot?
    # - Anzahl Tauschmöglichkeiten (pro Tag / Stunde / Minute?)
    # - Anzahl lukrativer Tauschmöglichkeiten (größer als x Prozent?)
    # - Darstellung Preis / Volumen / sonstige Aktivität / Events?
    
    # Einzelne Segmente auswerten
    for (segment in seq_along(intervals)) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        # ...
        
        # Beschreibende Statistiken
        # Siehe oben
        # ...
    }
    
    
    # BTC/EUR -----------------------------------------------------------------
    
    # Daten laden (~3 GB)
    comparablePrices <- loadComparablePricesByCurrencyPair("btceur")
    
    # Intervalle definieren
    breakpoints <- c("2016-03-01", "2017-01-01", "2018-03-01")
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    
    # Übersicht mit allen Segmenten anzeigen
    arbitrageIndex <- aggregateArbitrageIndex(comparablePrices, "1 month")
    plotArbitrageIndex(arbitrageIndex, breakpoints=breakpoints)
    
    # Beschreibende Statistiken
    # ...
    
    # Einzelne Segmente auswerten
    for (segment in seq_along(intervals)) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        # ...
        
        # Beschreibende Statistiken
        # ...
    }
    
    
    # BTC/GBP -----------------------------------------------------------------
    
    # Daten laden (~400 MB unkomprimiert)
    comparablePrices <- loadComparablePricesByCurrencyPair("btcgbp")
    
    # Intervalle definieren
    breakpoints <- c("2016-01-01", "2017-06-01", "2018-04-01", "2019-06-01")
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    
    # Übersicht mit allen Segmenten anzeigen
    arbitrageIndex <- aggregateArbitrageIndex(comparablePrices, "1 month")
    plotArbitrageIndex(arbitrageIndex, breakpoints=breakpoints)
    
    # Beschreibende Statistiken
    # ...
    
    # Einzelne Segmente auswerten
    for (segment in seq_along(intervals)) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        
        printf.debug(
            "%s Datenpunkte im Intervall von %s bis %s",
            format.number(nrow(comparablePrices[Time %between% segmentInterval])),
            format(segmentInterval[1], "%d.%m.%Y"),
            format(segmentInterval[2], "%d.%m.%Y")
        )
        
        # Variante 1: Aggregierte Liniengrafik: Nur sinnvoll, wenn keine/wenige Lücken
        arbitrageIndex <- aggregateArbitrageIndex(
            comparablePrices,
            floorUnits = "1 day",
            interval = segmentInterval
        )
        plotArbitrageIndex(arbitrageIndex)
        
        # Variante 2: Punktgrafik: Sinnvoll auch bei vielen Lücken, nicht aber
        # bei großen Datenmengen
        plotArbitrageIndex(
            arbitrageIndex = comparablePrices[Time %between% segmentInterval],
            plotType = "dots"
        )
        
        # Beschreibende Statistiken
        # ...
    }
    
    
    # BTC/JPY -----------------------------------------------------------------
    
    # Daten laden (wenige MB unkomprimiert)
    comparablePrices <- loadComparablePricesByCurrencyPair("btcjpy")
    
    # Intervalle definieren
    breakpoints <- c("2019-06-01", "2019-11-01")
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    
    # Übersicht mit allen Segmenten anzeigen
    arbitrageIndex <- aggregateArbitrageIndex(comparablePrices, "1 day")
    plotArbitrageIndex(arbitrageIndex, breakpoints=breakpoints)
    
    # Beschreibende Statistiken
    # ...
    
    # Einzelne Segmente auswerten
    for (segment in seq_along(intervals)) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        
        printf.debug(
            "%s Datenpunkte im Intervall von %s bis %s",
            format.number(nrow(comparablePrices[Time %between% segmentInterval])),
            format(segmentInterval[1], "%d.%m.%Y"),
            format(segmentInterval[2], "%d.%m.%Y")
        )
        
        # Variante 1: Aggregierte Liniengrafik: Nur sinnvoll, wenn keine/wenige Lücken
        arbitrageIndex <- aggregateArbitrageIndex(
            comparablePrices,
            floorUnits = "1 day",
            interval = segmentInterval
        )
        plotArbitrageIndex(arbitrageIndex)
        
        # Variante 2: Punktgrafik: Sinnvoll auch bei vielen Lücken, nicht aber
        # bei großen Datenmengen
        plotArbitrageIndex(
            arbitrageIndex = comparablePrices[Time %between% segmentInterval],
            plotType = "dots"
        )
        
        # Beschreibende Statistiken
        # ...
    }
    
}
