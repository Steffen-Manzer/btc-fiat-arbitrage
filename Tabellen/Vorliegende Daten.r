#' LaTeX-Tabelle zu den vorliegenden (und genutzten) Datensätzen
#' erzeugen. Enthält: Anzahl Ticks, Start- und Enddatum.


# Bibliotheken und Hilfsfunktionen laden --------------------------------------
library("fst")
library("readr") # read_file
library("stringr") # str_replace
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Konfiguration/FilePaths.r")


# Konfiguration ---------------------------------------------------------------

#' Börsen und deren offizielle Bezeichnung
exchanges <- list(
    "bitfinex"="Bitfinex",
    "bitstamp"="Bitstamp",
    "coinbase"="Coinbase Pro",
    "kraken"="Kraken",
    "dukascopy"="Dukascopy",
    "truefx"="TrueFX"
)

#' Genutzte Kurspaare (Kleinbuchstaben)
filterByPairs <- c("btcusd", "btceur", "eurusd")

#' Tabellen-Template mit `{tableContent}` als Platzhalter
templateFile <- sprintf("%s/Tabellen/Templates/Vorliegende_Datensaetze.tex", latexOutPath)

#' Zieldatei
outFile <- sprintf("%s/Tabellen/Vorliegende_Datensaetze.tex", latexOutPath)


# Konfiguration prüfen ----------------------------------------------------
stopifnot(file.exists(templateFile))
if (file.exists(outFile)) {
    Sys.chmod(outFile, mode="0644")
}


# Tabelle erzeugen ------------------------------------------------------------
tableContent <- ""
`%nin%` = Negate(`%in%`)

for (i in seq_along(exchanges)) {
    source <- names(exchanges)[i]
    exchangeName <- exchanges[[i]]
    
    pairs <- list.files(sprintf("Cache/%s", tolower(source)))
    for (pair in pairs) {
        
        # Nur gewählte Währungspaare in Tabelle aufnehmen
        if (tolower(pair) %nin% filterByPairs) {
            next
        }
        
        # Vorhandene Dateien finden - alphabetisch sortiert
        files <- list.files(sprintf("Cache/%s/%s/tick", source, pair), full.names=TRUE)
        
        numTicks <- 0L
        for (i in seq_along(files)) {
            f <- files[i]
            
            # Anzahl Ticks lesen
            fileStats <- metadata_fst(f)
            numTicks <- numTicks + fileStats$nrOfRows
            
            # Ersten Datensatz lesen
            if (i == 1L) {
                firstDataset <- read_fst(f, columns=c("Time"), to=1L, as.data.table=TRUE)
                firstDataset <- firstDataset$Time[1L]
            }
            
            # Letzten Datensatz lesen
            if (i == length(files)) {
                lastDataset <- read_fst(f, columns=c("Time"), 
                                        from=fileStats$nrOfRows, to=fileStats$nrOfRows,
                                        as.data.table=TRUE)
                lastDataset <- lastDataset$Time[1L]
            }
        }
        
        tableContent <- paste0(
            tableContent,
            sprintf("        %s &\n", exchangeName), # Börse / Datenquelle
            sprintf("            %s &\n", format.currencyPair(pair)), # Kurspaar
            sprintf("            %s &\n", format.number(numTicks)), # Anzahl Ticks
            sprintf("            %s &\n", format(firstDataset, "%d.%m.%Y %H:%M:%S")), # Von
            sprintf("            %s \\\\\n", format(lastDataset, "%d.%m.%Y %H:%M:%S")), # Bis
            "\n"
        )
    }
}


# Tabelle schreiben -------------------------------------------------------
templateFile |>
    read_file() |>
    str_replace(coll("{tableContent}"), tableContent) |>
    write_file(outFile)

# Vor versehentlichem Überschreiben schützen
Sys.chmod(outFile, mode="0444")
