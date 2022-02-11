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
        
        # Unterschiede berechnen
        priceDifferences[,PriceDifference:=(PriceHigh/PriceLow)-1]
        
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
            maxDifference <- priceDifferences[which.max(PriceDifference)]
            with(maxDifference, printf.debug(
                "Höchstwert: %s (%s <-> %s) am %s\n",
                format.number(PriceDifference),
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
        maxDifference <- combinedPriceDifferences[which.max(PriceDifference)]
        with(maxDifference, printf.debug(
            "Höchstwert: %s (%s <-> %s) am %s\n",
            format.number(PriceDifference),
            format.money(PriceLow),
            format.money(PriceHigh),
            formatPOSIXctWithFractionalSeconds(Time, "%d.%m.%Y %H:%M:%OS")
        ))
    }
    
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
    
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        tic()
    }
    
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
            n = .N
        ),
        by=.(Time=floor_date(Time, unit=floorUnits))
    ]
    printf.debug("Aggregation auf '%s' ergab %s Datensätze. ",
                 floorUnits, format.number(nrow(priceDifferences)))
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        toc()
    }
    
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


#' Zeichne Preisunterschiede als Linien-/Punktgrafik im Zeitverlauf
#' 
#' @param priceDifferences `data.table` mit den aggr. Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @param breakpoints Vektor mit Daten (Plural von: Datum) der Strukturbrüche
#' @param removeGaps Datenlücken nicht interpolieren/zeichnen
#' @param plotType Plot-Typ: line oder point
#' @return Der Plot (unsichtbar)
plotAggregatedPriceDifferencesOverTime <- function(
    priceDifferences,
    latexOutPath = NULL,
    breakpoints = NULL,
    removeGaps = TRUE,
    plotType = "line"
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
    plotYLab <- "Preisunterschied"
    
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
                aes(
                    xmin = From,
                    xmax = To,
                    ymin = 0,
                    ymax = maxValue * 1.05,
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
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
        coord_cartesian(ylim=c(0, maxValue)) +
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


#' Zeichne Preisunterschiede als Boxplot nach Börsenpaar gruppiert
#' 
#' @param comparablePrices `data.table` mit den Preisen der verschiedenen Börsen
#' @param latexOutPath Ausgabepfad als LaTeX-Datei
#' @return Der Plot (unsichtbar)
plotPriceDifferencesBoxplotByExchangePair <- function(
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
    plotXLab <- "Börsenpaar"
    plotYLab <- "Preisunterschied"
    
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
#'                         oder aus `aggregatePriceDifferences`
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
    numRowsTotal <- nrow(dataset)
    
    # Tabellenzeile erzeugen
    createRow <- function(numRows, dataSubset, end="\\\\\n\n") {
        intervalLengthHours <- 
            difftime(
                last(dataSubset$Time),
                first(dataSubset$Time),
                units = "hours"
            ) |>
            round() |>
            as.double()
        numRowsPerHour <- numRows / intervalLengthHours
        numRowsPerDay <- numRows / (intervalLengthHours / 24)
        numRowsLargerThan1_5Pct <- length(which(dataSubset$PriceDifference >= .015))
        numRowsLargerThan2_5Pct <- length(which(dataSubset$PriceDifference >= .025))
        numRowsLargerThan5Pct <- length(which(dataSubset$PriceDifference >= .05))
        s <- strrep(" ", 12) # Einrückung in der Ergebnisdatei
        return(paste0(
            sprintf("%s%% %s Datensätze pro Tag\n", s,
                    format.numberWithFixedDigits(numRowsPerDay, digits=1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRows),
                    format.percentage(numRows / numRowsTotal, 1L)),
            sprintf("%s%s &\n", s, format.numberWithFixedDigits(numRowsPerHour, 1)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan1_5Pct),
                    format.percentage(numRowsLargerThan1_5Pct / numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan2_5Pct),
                    format.percentage(numRowsLargerThan2_5Pct / numRows, 1L)),
            sprintf("%s\\makecell*[r]{%s\\\\(%s\\,\\%%)} &\n", s, 
                    format.number(numRowsLargerThan5Pct),
                    format.percentage(numRowsLargerThan5Pct / numRows, 1L)),
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
            numRows <- nrow(dataset[ExchangeHigh == exchange | ExchangeLow == exchange])
            if (numRows > 0L) {
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
                    createRow(
                        numRows, 
                        dataset[ExchangeHigh == exchange | ExchangeLow == exchange],
                        end = "\n"
                    ),
                    "        \\global\\rownum=2\\relax\\\\\n\n"
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
                            sprintf("        \\quad davon mit %s &\n", exchangeNames[[j]]),
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
    }
    
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
analysePriceDifferences <- function(pair, breakpoints)
{
    # Vorherige Berechnungen ggf. aus dem Speicher bereinigen
    gc()
    
    # Daten laden
    comparablePrices <- loadComparablePricesByCurrencyPair(pair)
    
    # Boxplot für gesamten Zeitraum erstellen
    plotPriceDifferencesBoxplotByExchangePair(
        comparablePrices,
        latexOutPath = sprintf(
            "%s/Abbildungen/Empirie_Raumarbitrage_%s_UebersichtBoxplot.tex",
            latexOutPath, toupper(pair)
        )
    )
    
    # Liniengrafik für gesamten Zeitraum erstellen
    aggregatedPriceDifferences <- aggregatePriceDifferences(comparablePrices, "1 month")
    plotAggregatedPriceDifferencesOverTime(
        aggregatedPriceDifferences,
        breakpoints = breakpoints,
        removeGaps = FALSE, # Lücken werden auf 1d-Basis entfernt, daher hier nicht
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
            "Zentrale Kenngrößen der Preisunterschiede für %s im Gesamtüberblick",
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
        aggregatedPriceDifferences <- aggregatePriceDifferences(
            comparablePrices,
            floorUnits = "1 day",
            interval = segmentInterval
        )
        
        if (nrow(aggregatedPriceDifferences) > 50L) {
            
            # Variante 1: Aggregierte Liniengrafik: Nur sinnvoll, wenn keine/wenige Lücken
            plotAggregatedPriceDifferencesOverTime(
                aggregatedPriceDifferences,
                latexOutPath = sprintf(
                    "%s/Abbildungen/Empirie_Raumarbitrage_%s_Uebersicht_%d.tex",
                    latexOutPath, toupper(pair), segment
                )
            )
            
        } else {
            
            # Variante 2: Punktgrafik: Sinnvoll auch bei vielen Lücken, nicht aber
            # bei großen Datenmengen. Zeichnet *alle* Daten, nicht aggregierte Daten
            plotAggregatedPriceDifferencesOverTime(
                comparablePrices[Time %between% segmentInterval],
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
                "Zentrale Kenngrößen der Preisunterschiede für %s von %s bis %s",
                format.currencyPair(pair),
                format(segmentInterval[1], "%d.%m.%Y"),
                format(segmentInterval[2], "%d.%m.%Y")
            ),
            label = sprintf(
                "Empirie_Raumarbitrage_%s_Ueberblick_%d",
                toupper(pair), segment
            )
        )
        
        # Boxplot mit allen Daten
        plotPriceDifferencesBoxplotByExchangePair(
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
    
    # Die Breakpoints selbst werden immer dem letzten der beiden entstehenden
    # Intervalle zugerechnet
    
    # BTC/USD
    # Datenmenge: ~11,5 GB
    analysePriceDifferences(
        pair = "btcusd",
        breakpoints = 
            c("2014-03-01", "2017-01-01", "2018-06-01", "2019-07-01", "2020-05-01")
    ) ; invisible(gc())
    
    # BTC/EUR
    # Datenmenge: ~4,5 GB
    analysePriceDifferences(
        pair = "btceur",
        breakpoints = c("2016-04-01", "2017-01-01", "2018-03-01")
    ) ; invisible(gc())
    
    # BTC/GBP
    # Datenmenge: ~450 MB
    analysePriceDifferences(
        pair = "btcgbp",
        breakpoints = c("2016-01-01", "2017-06-01", "2018-03-29", "2019-06-01")
    ) ; invisible(gc())
    
    # BTC/JPY
    # Datenmenge: < 10 MB
    analysePriceDifferences(
        pair = "btcjpy",
        breakpoints = c("2019-06-01", "2019-11-01")
    ) ; invisible(gc())
}
