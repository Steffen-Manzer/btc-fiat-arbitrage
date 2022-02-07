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


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.r")
DEBUG_PRINT <- TRUE; source("Funktionen/printf.r")
source("Konfiguration/FilePaths.r")
library("fst")
library("data.table")
library("lubridate") # floor_date
library("ggplot2")
library("ggthemes")
library("readr") # read_file
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

#' Tabellen-Template mit `{tableContent}`, `{tableCaption` und `{tableLabel}` 
#' als Platzhalter
summaryTableTemplateFile <- 
    sprintf("%s/Tabellen/Templates/Empirie_Raumarbitrage_Uebersicht_nach_Boerse.tex",
            latexOutPath)


# Hilfsfunktionen -------------------------------------------------------------

#' Lade alle vergleichbaren Preise für ein Kurspaar nach Börse
#' 
#' @param currencyPair Kurspaar (z.B. BTCUSD)
#' @param exchange Börse (z.B. bitfinex)
#' @return `data.table` mit den Preisunterschieden
loadComparablePricesByCurrencyPairAndExchange <- function(currencyPair, exchange)
{
    stop("Unbenutzt?")
    
    # Parameter validieren
    stopifnot(
        is.character(currencyPair), length(currencyPair) == 1L,
        is.character(exchange), length(exchange) == 1L,
        nchar(currencyPair) == 6L
    )
    
    # Nur jeweils erste Datei einlesen, siehe oben.
    sourceFiles <- list.files(
        "Cache/Raumarbitrage",
        pattern=sprintf("^%s-.*%s.*-1\\.fst$", currencyPair, exchange),
        full.names = TRUE
    )
    
    # Variablen initialisieren / leeren
    combinedPriceDifferences <- NULL
    
    # Alle Datensätze einlesen
    for (i in seq_along(sourceFiles)) {
        
        # Dateiname bestimmen
        sourceFile <- sourceFiles[[i]]
        
        # Datei lesen
        priceDifferences <- read_fst(sourceFile, as.data.table=TRUE)
        
        # Statistiken ausgeben
        if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
            printf.debug(
                "%s (%s) mit %s Datensätzen von %s bis %s.\n", 
                basename(sourceFile),
                format(object.size(priceDifferences), units="auto", standard="SI"),
                format.number(nrow(priceDifferences)),
                format(first(priceDifferences$Time), "%d.%m.%Y"),
                format(last(priceDifferences$Time), "%d.%m.%Y")
            )
            maxIndexValue <- priceDifferences[which.max(ArbitrageIndex)]
            with(maxIndexValue, printf.debug(
                "Höchstwert: %s (%s <-> %s) am %s\n",
                format.number(ArbitrageIndex),
                format.money(PriceLow),
                format.money(PriceHigh),
                formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
            ))
        }
        
        if (i > 1L) {
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
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        printf.debug(
            "\nKombination aller Daten ergab %s Datensätze (%s) von %s bis %s.\n",
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
            formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
        ))
    }
    
    combinedPriceDifferences[,Exchange:=exchangeNames[[exchange]] ]
    
    return(combinedPriceDifferences)
}


#' Lade alle vergleichbaren Preise für ein Kurspaar
#' 
#' @param currencyPair Kurspaar (z.B. BTCUSD)
#' @return `data.table` mit den Preisunterschieden
loadComparablePricesByCurrencyPair <- function(currencyPair)
{
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
        
        # Dateiname bestimmen
        sourceFile <- sourceFiles[[i]]
        
        # Datei lesen
        priceDifferences <- read_fst(sourceFile, as.data.table=TRUE)
        
        # Arbitrageindex berechnen
        priceDifferences[,ArbitrageIndex:=PriceHigh/PriceLow]
        
        # Statistiken ausgeben
        if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
            printf.debug(
                "%s (%s) mit %s Datensätzen von %s bis %s.\n", 
                basename(sourceFile),
                format(object.size(priceDifferences), units="auto", standard="SI"),
                format.number(nrow(priceDifferences)),
                format(first(priceDifferences$Time), "%d.%m.%Y"),
                format(last(priceDifferences$Time), "%d.%m.%Y")
            )
            maxIndexValue <- priceDifferences[which.max(ArbitrageIndex)]
            with(maxIndexValue, printf.debug(
                "Höchstwert: %s (%s <-> %s) am %s\n",
                format.number(ArbitrageIndex),
                format.money(PriceLow),
                format.money(PriceHigh),
                formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
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
    
    # Sortieren
    setorder(combinedPriceDifferences, Time)
    
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
            formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
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


#' Zeichne Arbitrageindex
#' 
#' @param arbitrageIndex `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param plotType Plot-Typ: line oder point
#' @return Der Plot (unsichtbar)
plotAggregatedArbitrageIndexOverTime <- function(
    arbitrageIndex,
    latexOutPath = NULL,
    breakpoints = NULL,
    plotType = "line"
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
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        
        plotTextPrefix <- "\\footnotesize "
    } else {
        plotTextPrefix <- ""
    }
    
    # Einige Bezeichnungen
    plotXLab <- "Datum"
    plotYLab <- "Arbitrageindex"
    
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
                    label = paste0(plotTextPrefix, Set)
                ),
                data = intervals
            )
    }
    
    if (plotType == "line") {
        
        # Liniengrafik. Nützlich, wenn keine Lücken in den Daten vorhanden sind
        plot <- plot +
            # Q1/Q3 zeichnen
            geom_ribbon(aes(x=Time, ymin=Q1, ymax=Q3), fill="grey70") +
            
            # Median zeichnen
            geom_line(aes(x=Time, y=Median, color="1", linetype="1"), size=.5)
        
    } else if (plotType == "point") {
        
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
        scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
        coord_cartesian(ylim=c(1, maxValue)) +
        scale_y_continuous(
            labels = function(x) paste(format.number(x * 100), "%")
        ) +
        scale_color_ptol() +
        scale_fill_ptol() + 
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    # Plot zeichnen
    print(plot)
    
    if (!is.null(latexOutPath)) {
        dev.off()
    }
    
    return(invisible(plot))
}


#' Zeichne Preisunterschiede
#' 
#' @param comparablePrices `data.table` mit den Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @return Der Plot (unsichtbar)
plotArbitrageIndexByExchange <- function(
    comparablePrices,
    latexOutPath = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(comparablePrices), nrow(comparablePrices) > 0L,
        is.null(latexOutPath) || (is.character(latexOutPath) && length(latexOutPath) == 1L)
    )
    
    # Ausgabeoptionen
    if (!is.null(latexOutPath)) {
        source("Konfiguration/TikZ.r")
        printf.debug("Ausgabe als LaTeX in Datei %s\n", basename(latexOutPath))
        tikz(
            file = latexOutPath,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        
        plotTextPrefix <- "\\footnotesize "
    } else {
        plotTextPrefix <- ""
    }
    
    # Achsenbeschriftung
    plotXLab <- "Börse"
    plotYLab <- "Arbitrageindex"
    
    # Zeichnen
    if (!is.null(comparablePrices$Q3)) {
        maxValue <- max(comparablePrices$Q3)
    } else if (!is.null(comparablePrices$ArbitrageIndex)) {
        maxValue <- max(comparablePrices$ArbitrageIndex)
    } else {
        stop("Keine Quartile und keine Rohdaten gefunden!")
    }
    
    
    # Werte berechnen: Wesentlich schneller,
    # als geom_boxplot die Berechnung übernehmen zu lassen.
    # Das grafische Ergebnis ist identisch, da ohnehin 
    # keine Ausreißer angezeigt werden.
    boxplot_stats <- data.table()
    for (i in seq_along(exchangeNames)) {
        exchange <- names(exchangeNames)[[i]]
        exchangeSummary <- summary(
            comparablePrices[
                ExchangeHigh == exchange | ExchangeLow == exchange,
                ArbitrageIndex
            ]
        )
        if (is.na(exchangeSummary[[1]])) {
            next
        }
        iqr <- exchangeSummary[[5]] - exchangeSummary[[2]]
        boxplot_stats <- rbindlist(list(boxplot_stats, data.table(
            Exchange = exchangeNames[[i]],
            ColorGroup = "Börse",
            min = max(exchangeSummary[[1]], exchangeSummary[[2]] - 1.5*iqr),
            lower = exchangeSummary[[2]],
            middle = exchangeSummary[[3]],
            upper = exchangeSummary[[5]],
            max = min(exchangeSummary[[6]], exchangeSummary[[5]] + 1.5*iqr)
        )))
    }
    
    # Globale Statistiken
    totalSummary <- summary(comparablePrices$ArbitrageIndex)
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
    boxplot_stats[,
        Exchange:=factor(
          Exchange,
          levels=c("Gesamt", exchangeNames |> unlist() |> unname())
        )
    ]
    
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
            aes(x = Exchange,
                group = Exchange,
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
                x = Exchange,
                group = Exchange,
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
            labels = function(x) paste(format.number(x * 100), "%"),
            limits = c(1, max(boxplot_stats$max)*1.0005)
        ) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_color_ptol() +
        scale_fill_ptol() + 
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
        )
    
    # Plot zeichnen
    print(plot)
    
    if (!is.null(latexOutPath)) {
        dev.off()
    }
    
    return(invisible(plot))
}


#' Informationen über einen Datensatz als LaTeX-Tabelle ausgeben
#' 
#' @param comparablePrices `data.table` aus `loadComparablePricesByCurrency`
#'                         oder aus `aggregateArbitrageIndex`
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
    printf("Erzeuge Überblickstabelle in %s\n", basename(outFile))
    format.percentage <- function(d, digits=3L)
        formatC(d*100, digits=digits, format="f", decimal.mark=",", big.mark=".")
    
    numRowsTotal <- nrow(dataset)
    
    # Tabellenzeile erzeugen
    createRow <- function(numRows, dataSubset) {
        intervalLengthHours <- 
            difftime(
                last(dataSubset$Time),
                first(dataSubset$Time),
                units = "hours"
            ) |>
            round() |>
            as.double()
        numRowsPerHour <- numRows / intervalLengthHours
        numRowsLargerThan2Pct <- length(which(dataSubset$ArbitrageIndex >= 1.02))
        numRowsLargerThan5Pct <- length(which(dataSubset$ArbitrageIndex >= 1.05))
        numRowsLargerThan10Pct <- length(which(dataSubset$ArbitrageIndex >= 1.1))
        s <- strrep(" ", 12) # Einrückung in der Ergebnisdatei
        return(paste0(
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRows),
                    format.percentage(numRows / numRowsTotal, 1L)),
            sprintf("%s%s &\n", s, format.numberWithFixedDigits(numRowsPerHour, 1)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan2Pct),
                    format.percentage(numRowsLargerThan2Pct / numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan5Pct),
                    format.percentage(numRowsLargerThan5Pct / numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan10Pct),
                    format.percentage(numRowsLargerThan10Pct / numRows, 1L)),
            sprintf("%s%s\\,\\%% \\\\\n\n", s, 
                    format.percentage(max(dataSubset$ArbitrageIndex), 1))
        ))
    }
    
    tableContent <- ""
    
    # Jede Börse durchgehen
    for (i in seq_along(exchangeNames)) {
        exchange <- names(exchangeNames)[[i]]
        numRows <- nrow(dataset[ExchangeHigh==exchange|ExchangeLow==exchange])
        if (numRows > 0L) {
            
            # Gesamtüberblick für diese Börse
            tableContent <- paste0(
                tableContent,
                sprintf("        \\textbf{%s} &\n", exchangeNames[[i]]),
                createRow(
                    numRows, 
                    dataset[ExchangeHigh==exchange|ExchangeLow==exchange]
                )
            )
            
            # Paarweise mit anderen Börsen
            for (j in seq_along(exchangeNames)) {
                if (i == j) {
                    next
                }
                otherExchange <- names(exchangeNames)[[j]]
                numRows_withOtherExchange <- nrow(dataset[
                    (ExchangeHigh == exchange & ExchangeLow == otherExchange) |
                    (ExchangeHigh == otherExchange & ExchangeLow == exchange)
                ])
                if (numRows_withOtherExchange > 0L) {
                    tableContent <- paste0(
                        tableContent,
                        sprintf("        \\qquad davon mit %s &\n", exchangeNames[[j]]),
                        createRow(
                            numRows_withOtherExchange, 
                            dataset[
                                (ExchangeHigh == exchange & ExchangeLow == otherExchange) |
                                (ExchangeHigh == otherExchange & ExchangeLow == exchange)
                            ]
                        )
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
        str_replace(coll("{tableCaption}"), caption) |>
        str_replace(coll("{tableContent}"), tableContent) |>
        str_replace(coll("{tableLabel}"), label) |>
        write_file(outFile)
    
    # Vor versehentlichem Überschreiben schützen
    Sys.chmod(outFile, mode="0444")
    
    return(invisible(NULL))
}


# Haupt-Auswertungsfunktion ---------------------------------------------------
#' Preisunterschiede auf Arbitragemöglichkeiten abklopfen
analyseArbitrageIndex <- function(pair, breakpoints)
{
    # Vorherige Berechnungen ggf. aus dem Speicher bereinigen
    gc()
    
    # Daten laden
    comparablePrices <- loadComparablePricesByCurrencyPair(pair)
    
    # Doppelte Tauschmöglichkeiten zum selben Zeitpunkt entfernen
    # Dies mindert das "Problem" der doppelten Erfassung des selben Ticks
    # TODO Prüfen!!!
    comparablePrices <- comparablePrices[
        j=.(
            IDHigh = IDHigh[which.max(ArbitrageIndex)],
            PriceHigh = PriceHigh[which.max(ArbitrageIndex)],
            ExchangeHigh = ExchangeHigh[which.max(ArbitrageIndex)],
            IDLow = IDLow[which.max(ArbitrageIndex)],
            PriceLow = PriceLow[which.max(ArbitrageIndex)],
            ExchangeLow = ExchangeLow[which.max(ArbitrageIndex)],
            ArbitrageIndex = min(ArbitrageIndex)
        ),
        by = Time
    ]
    
    # Boxplot für gesamten Zeitraum erstellen
    plotArbitrageIndexByExchange(
        comparablePrices,
        latexOutPath = sprintf(
            "%s/Abbildungen/Empirie_Raumarbitrage_%s_UebersichtBoxplot.tex",
            latexOutPath, toupper(pair)
        )
    )
    
    # Liniengrafik für gesamten Zeitraum erstellen
    arbitrageIndex <- aggregateArbitrageIndex(comparablePrices, "1 month")
    plotAggregatedArbitrageIndexOverTime(
        arbitrageIndex,
        breakpoints = breakpoints,
        latexOutPath = sprintf(
            "%s/Abbildungen/Empirie_Raumarbitrage_%s_Uebersicht.tex",
            latexOutPath, toupper(pair)
        )
    )
    
    # Beschreibende Statistiken
    summariseDatasetAsTable(
        comparablePrices,
        outFile = sprintf(
            "%s/Tabellen/Empirie_Raumarbitrage_%s_Uebersicht.tex",
            latexOutPath, toupper(pair)
        ),
        caption = sprintf(
            "Zentrale Kenngrößen des Arbitrageindex für %s im Gesamtüberblick",
            format.currencyPair(pair)
        ),
        label = sprintf("Empirie_Raumarbitrage_%s_Ueberblick", toupper(pair))
    )
    
    # TODO
    # - Anzahl Tauschmöglichkeiten (pro Tag / Stunde / Minute?)
    # - Anzahl lukrativer Tauschmöglichkeiten (größer als x Prozent?)
    # - Darstellung Preis / Volumen / sonstige Aktivität / Events?
    
    # Intervalle bestimmen
    intervals <- calculateIntervals(comparablePrices$Time, breakpoints)
    
    # Einzelne Segmente auswerten
    for (segment in seq_len(nrow(intervals))) {
        segmentInterval <- c(intervals$From[segment], intervals$To[segment])
        
        printf.debug(
            "%s Datenpunkte im Intervall von %s bis %s.\n",
            format.number(nrow(comparablePrices[Time %between% segmentInterval])),
            format(segmentInterval[1], "%d.%m.%Y"),
            format(segmentInterval[2], "%d.%m.%Y")
        )
        
        # Daten auf einen Tag aggregieren
        arbitrageIndex <- aggregateArbitrageIndex(
            comparablePrices,
            floorUnits = "1 day",
            interval = segmentInterval
        )
        
        if (nrow(arbitrageIndex) > 200) {
            
            # Variante 1: Aggregierte Liniengrafik: Nur sinnvoll, wenn keine/wenige Lücken
            plotAggregatedArbitrageIndexOverTime(
                arbitrageIndex,
                latexOutPath = sprintf(
                    "%s/Abbildungen/Empirie_Raumarbitrage_%s_Uebersicht_%d.tex",
                    latexOutPath, toupper(pair), segment
                )
            )
            
        } else {
            
            # Variante 2: Punktgrafik: Sinnvoll auch bei vielen Lücken, nicht aber
            # bei großen Datenmengen
            plotAggregatedArbitrageIndexOverTime(
                arbitrageIndex = comparablePrices[Time %between% segmentInterval],
                plotType = "point",
                latexOutPath = sprintf(
                    "%s/Abbildungen/Empirie_Raumarbitrage_%s_Uebersicht_%d.tex",
                    latexOutPath, toupper(pair), segment
                )
            )
            
        }
        
        # Statistiken in Tabelle ausgeben
        # -> Wie für gesamten Datensatz
        # ...
        summariseDatasetAsTable(
            comparablePrices[Time %between% segmentInterval],
            outFile = sprintf(
                "%s/Tabellen/Empirie_Raumarbitrage_%s_Uebersicht_%d.tex",
                latexOutPath, toupper(pair), segment
            ),
            caption = sprintf(
                "Zentrale Kenngrößen des Arbitrageindex für %s von %s bis %s",
                format.currencyPair(pair),
                format(segmentInterval[1], "%d.%m.%Y"),
                format(segmentInterval[2], "%d.%m.%Y")
            ),
            label = sprintf(
                "Empirie_Raumarbitrage_%s_Ueberblick_%d",
                toupper(pair), segment
            )
        )
        
        # Boxplot
        plotArbitrageIndexByExchange(
            comparablePrices[Time %between% segmentInterval],
            latexOutPath = sprintf(
                "%s/Abbildungen/Empirie_Raumarbitrage_%s_UebersichtBoxplot_%d.tex",
                latexOutPath, toupper(pair), segment
            )
        )
    }
}


# Auswertung (grafisch, numerisch) --------------------------------------------

# Händisch einzeln bei Bedarf starten, da große Datenmengen geladen werden
# und die Verarbeitung viel Zeit in Anspruch nimmt.
if (FALSE) {
    
    # BTC/USD
    # Datenmenge: ~11,5 GB
    analyseArbitrageIndex(
        pair = "btcusd",
        breakpoints = 
            c("2014-03-01", "2017-01-01", "2018-06-01", "2019-07-01", "2020-05-01")
    ) ; invisible(gc())
    
    # BTC/EUR
    # Datenmenge: ~4,5 GB
    analyseArbitrageIndex(
        pair = "btceur",
        breakpoints = c("2016-04-01", "2017-01-01", "2018-03-01")
    ) ; invisible(gc())
    
    # BTC/GBP
    # Datenmenge: ~450 MB
    analyseArbitrageIndex(
        pair = "btcgbp",
        breakpoints = c("2016-01-01", "2017-06-01", "2018-04-01", "2019-06-01")
    ) ; invisible(gc())
    
    # BTC/JPY
    # Datenmenge: < 10 MB
    analyseArbitrageIndex(
        pair = "btcjpy",
        breakpoints = c("2019-06-01", "2019-11-01")
    ) ; invisible(gc())
}
