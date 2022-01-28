#' Stelle alle Filter-Zeiträume aus `Daten/bitcoin-metadata.json` in einer
#' Tabelle dar.


# Bibliotheken und Hilfsfunktionen laden ----------------------------------
library("rjson")
library("readr") # read_file
library("stringr") # str_replace
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatDuration.r")
source("Konfiguration/FilePaths.r")


# Konfiguration -----------------------------------------------------------

#' Börsen und deren offizielle Bezeichnung
exchangeNames <- list(
    "bitfinex"="Bitfinex",
    "bitstamp"="Bitstamp",
    "coinbase"="Coinbase Pro",
    "kraken"="Kraken"
)

#' Genutzte Kurspaare (Kleinbuchstaben)
#' TODO Anpassen an tatsächlich genutzte Paare
requiredPairs <- c("btcusd", "btceur", "btcgbp", "btcjpy")

#' Tabellen-Template mit `{tableContent}` als Platzhalter
templateFile <- sprintf("%s/Tabellen/Templates/Bitcoin_Empirie_Filter.tex", latexOutPath)

#' Zieldatei
outFile <- sprintf("%s/Tabellen/Bitcoin_Empirie_Filter.tex", latexOutPath)


# Konfiguration prüfen ----------------------------------------------------
stopifnot(file.exists(templateFile))
if (file.exists(outFile)) {
    Sys.chmod(outFile, mode="0644")
}


# Metadaten lesen ---------------------------------------------------------
exchangeMetadata <- fromJSON(file="Daten/bitcoin-metadata.json")
tableContent <- ""
`%nin%` = Negate(`%in%`)

for (i in seq_along(exchangeMetadata)) {
    exchangeName <- exchangeNames[[
        names(exchangeMetadata)[i]
    ]]
    exchangeData <- exchangeMetadata[[i]]
    tableContent <- paste0(
        tableContent,
        "    \\rowcolor{white}\n",
        sprintf("    \\multicolumn{4}{@{}l@{}}{\\textbf{%s}}\n", exchangeName),
        "    \\global\\rownum=1\\relax\\\\\n\n"
    )
    
    for (j in seq_along(exchangeData)) {
        pairRaw <- names(exchangeData)[j]
        if (tolower(pairRaw) %nin% requiredPairs) {
            next
        }
        pair <- format.currencyPair(pairRaw)
        pairData <- exchangeData[[j]]
        for (k in seq_along(pairData$suspiciousPeriods)) {
            period <- pairData$suspiciousPeriods[[k]]
            
            if (period$filter == FALSE) {
                next
            }
            
            # Dauer berechnen
            periodDuration <- difftime(period$endDate, period$startDate, units="secs") |>
                format.duration() |>
                # Leerzeichen zwischen Zahl und Einheit durch schmales Leerzeichen ersetzen
                # 1 min, 2 s -> 1\,min, 2\,s
                str_replace_all("([0-9]+) ", "\\1\\\\,")
            
            tableContent <- paste0(
                tableContent,
                sprintf("    \\qquad %s &\n", pair),
                sprintf("        %s &\n", format(as.POSIXct(period$startDate), "%d.%m.%Y, %H:%M:%S")),
                sprintf("        %s &\n", format(as.POSIXct(period$endDate), "%d.%m.%Y, %H:%M:%S")),
                sprintf("        %s &\n", periodDuration),
                sprintf("        %s\n", period$notes),
                "        \\\\\n\n"
            )
        }
    }
}


# Tabelle schreiben -------------------------------------------------------
templateFile |>
    read_file() |>
    str_replace(coll("{tableContent}"), tableContent) |>
    write_file(outFile)

# Vor versehentlichem Überschreiben schützen
Sys.chmod(outFile, mode="0444")
