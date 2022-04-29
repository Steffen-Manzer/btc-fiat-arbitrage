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
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")
library("fst")
library("data.table")
library("lubridate") # floor_date
library("ggplot2")
library("ggthemes")
library("gridExtra") # grid.arrange
library("readr") # read_file, write_file
#library("scales") # breaks_extended
library("stringr") # str_replace
library("tictoc")


# Konfiguration -----------------------------------------------------------
plotAsLaTeX <- FALSE
exchangeNames <- list(
    "bitfinex" = "Bitfinex",
    "bitstamp" = "Bitstamp",
    "coinbase" = "Coinbase Pro",
    "kraken" = "Kraken"
)

# TODO.
# Die Breakpoints selbst werden immer dem letzten der beiden entstehenden
# Intervalle zugerechnet
breakpointsByExchange <- list(
    "bitfinex" = NULL,
    "bitstamp" = NULL,
    "coinbase" = NULL,
    "kraken" = NULL
)

#' Tabellen-Template mit `{tableContent}`, `{tableCaption` und `{tableLabel}` 
#' als Platzhalter
summaryTableTemplateFile <- 
    sprintf("%s/Tabellen/Templates/Dreiecksarbitrage_Uebersicht_nach_Boerse.tex",
            latexOutPath)

# Variablen für Tests initialisieren
exchange <- "coinbase"
currency_a <- "usd"
currency_b <- "eur"

# Ergebnisse für Test-/Entwicklungszwecke in globale Umgebung exportieren,
# um nicht jedes Mal Daten neu einlesen und berechnen zu müssen
DEBUG_ASSIGN_TO_GLOBAL_ENV <- TRUE


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
        Min = min(BestResult),
        Q1 = quantile(BestResult, probs=.25, names=FALSE),
        Mean = mean(BestResult),
        Median = median(BestResult),
        Q3 = quantile(BestResult, probs=.75, names=FALSE),
        Max = max(BestResult),
        n = .N
    ))
    
    if (!is.null(interval)) {
        
        # Zeitraum begrenzt
        # TODO Test
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
#' @param removeGaps Datenlücken nicht interpolieren/zeichnen
#' @param plotType Plot-Typ: line oder point
#' @param plotTitle Überschrift (optional)
#' @return Der Plot (unsichtbar)
plotAggregatedResultsOverTime <- function(
    arbitrageResults,
    latexOutPath = NULL,
    breakpoints = NULL,
    removeGaps = TRUE,
    plotType = "line",
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
    if (!is.null(arbitrageResults$Q3)) {
        minValue <- min(arbitrageResults$Q1)
        maxValue <- max(arbitrageResults$Q3)
    } else if (!is.null(arbitrageResults$BestResult)) {
        minValue <- min(arbitrageResults$BestResult)
        maxValue <- max(arbitrageResults$BestResult)
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
            gapGroups <- c(0, cumsum(diff(arbitrageResults$Time) > 2))
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
            geom_point(aes(x=Time, y=BestResult, color="1"), size=.25)
        
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
        coord_cartesian(ylim=c(minValue, maxValue)) +
        scale_y_continuous(
            labels = function(x) paste(format.number((x-1) * 100), "%")
        ) +
        scale_color_ptol() +
        scale_fill_ptol() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab)
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
    timeHorizon = "Monatliche",
    plotTitle = NULL
) {
    # Parameter validieren
    stopifnot(
        is.data.table(arbitrageResults), nrow(arbitrageResults) > 0L,
        !is.null(arbitrageResults$Time)
    )
    
    # Einige Bezeichnungen und Variablen
    plotXLab <- "Datum"
    plotYLab <- paste0(timeHorizon, " Beobachtungen")
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
        scale_color_ptol() +
        scale_fill_ptol() +
        labs(
            x = paste0(plotTextPrefix, plotXLab),
            y = paste0(plotTextPrefix, plotYLab, " [", roundedTo, "]")
        )
    
    if (!is.null(plotTitle)) {
        plot <- plot + ggtitle(paste0(plotSmallPrefix, plotTitle))
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
        numRowsLargerThan_A = length(which(result$data$BestResult >= 1.01)),
        numRowsLargerThan_B = length(which(result$data$BestResult >= 1.02)),
        numRowsLargerThan_C = length(which(result$data$BestResult >= 1.05)),
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
#' @param exchange Die zu betrachtende Börse
#' @param currency_a Gegenwährung 1
#' @param currency_b Gegenwährung 2
#' @param threshold Zeitliche Differenz zweier BTC-Ticks in Sekunden,
#'                  ab der das Tick-Paar verworfen wird.
#' @return `result` (invisible) Instanz der Klasse `TriangularResult`
analyseTriangularArbitrage <- function(
    exchange,
    currency_a,
    currency_b,
    threshold = 5L
)
{
    # Parameter validieren
    stopifnot(
        is.character(exchange), length(exchange) == 1L,
        is.character(currency_a), length(currency_a) == 1L, nchar(currency_a) == 3L,
        is.character(currency_b), length(currency_b) == 1L, nchar(currency_b) == 3L,
        is.numeric(threshold), length(threshold) == 1L
    )
    
    # Verzeichnisse anlegen
    plotOutPath <- sprintf(
        "%s/Abbildungen/Dreiecksarbitrage/%ds/btc-eur-usd/%s",
        latexOutPath, threshold, exchange
    )
    for (d in c(plotOutPath)) {
        if (!dir.exists(d)) {
            dir.create(d, recursive=TRUE)
        }
    }
    
    # Variablen initialisieren
    exchangeName <- exchangeNames[[exchange]]
    breakpoints <- breakpointsByExchange[[exchange]]
    
    dataFile <- sprintf(
        "Cache/Dreiecksarbitrage %ds/%s-%s-%s-1.fst",
        threshold, exchange, currency_a, currency_b
    )
    stopifnot(file.exists(dataFile))
    
    # Daten einlesen
    if (
        exists("DEBUG_ASSIGN_TO_GLOBAL_ENV") && isTRUE(DEBUG_ASSIGN_TO_GLOBAL_ENV) &&
        exists("result") && inherits(result, "TriangularResult") &&
        result$Exchange == exchange
    ) {
        # Entwicklungsmodus aktiv: Daten bereits geladen
        printf("Nutze bereits eingelesene Daten für %s.\n", exchangeName)
        
    } else {
        
        # Daten neu einlesen
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
        
        # Entwicklungsmodus aktiv, Werte für weitere Entwicklung beibehalten
        if (exists("DEBUG_ASSIGN_TO_GLOBAL_ENV") && isTRUE(DEBUG_ASSIGN_TO_GLOBAL_ENV)) {
            result <<- result
        }
    }
    
    # Zusammenfassung ausgeben
    numTotal <- nrow(result$data)
    printf("%s Datensätze. Davon:\n", format.number(numTotal))
    for (largerThan in c(5, 2, 1, .5)) {
        numLarger <- length(result$data[BestResult >= (1 + largerThan / 100), which=TRUE])
        printf(
            "Anzahl >= %.1f %%: %s (%s %%)\n",
            largerThan, format.number(numLarger), format.percentage(numLarger/numTotal, 1L)
        )
    }
    
    # Monatsdaten berechnen
    resultsByMonth <- aggregateResultsByTime(result, "1 month")
    
    # Überblicksgrafik
    p_diff <- plotAggregatedResultsOverTime(
        resultsByMonth,
        breakpoints = breakpoints,
        # Lücken werden immer auf 1d-Basis entfernt.
        # Da es sich um Monatsdaten handelt, wäre der Plot leer...
        removeGaps = FALSE,
        plotTitle = "Arbitrageergebnis"
    )
    p_nrow <- plotNumResultsOverTime(
        resultsByMonth,
        breakpoints = breakpoints,
        timeHorizon = "",
        plotTitle = "Anzahl monatlicher Beobachtungen"
    )
    
    # Als LaTeX-Dokument ausgeben
    source("Konfiguration/TikZ.R")
    tikz(
        file = sprintf("%s/Uebersicht.tex", plotOutPath),
        width = documentPageWidth,
        height = 11 / 2.54,
        sanitize = TRUE
    )
    grid.arrange(
        p_diff, p_nrow,
        layout_matrix = rbind(c(1),c(2))
    )
    dev.off()
    
    return(collectDatasetSummary(result))
}


# Auswertung händisch starten
if (FALSE) {
    
    bitfinexSummary <- analyseTriangularArbitrage("bitfinex", "usd", "eur") ; invisible(gc())
    bitstampSummary <- analyseTriangularArbitrage("bitstamp", "usd", "eur") ; invisible(gc())
    coinbaseSummary <- analyseTriangularArbitrage("coinbase", "usd", "eur") ; invisible(gc())
    krakenSummary <- analyseTriangularArbitrage("kraken", "usd", "eur") ; invisible(gc())
    
    # Zusammenfassende Tabelle erstellen
    totalSummary <- rbindlist(list(
        bitfinexSummary, bitstampSummary, coinbaseSummary, krakenSummary
    ))
    # TODO Pfad auf btc-eur-usd korrigieren (Angleich an Plots)
    tableOutPath <- sprintf(
        "%s/Tabellen/Dreiecksarbitrage/5s/BTCEURUSD/Uebersicht.tex",
        latexOutPath
    )
    
    if (!dir.exists(dirname(tableOutPath))) {
        dir.create(dirname(tableOutPath), recursive=TRUE)
    }
    
    summariseDatasetAsTable(
        totalSummary,
        outFile = tableOutPath,
        caption = sprintf(
            "Zentrale Kenngrößen der Dreiecksarbitrage für BTC, EUR und USD im Gesamtüberblick"
        ),
        label = sprintf("Dreiecksarbitrage_BTCEURUSD_Uebersicht_5s")
    )
    
}
