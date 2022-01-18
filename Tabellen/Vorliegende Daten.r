#' LaTeX-Tabelle zu den vorliegenden (und genutzten) Datensätzen
#' erzeugen. Enthält: Anzahl Ticks, Start- und Enddatum.
#' TODO Automatisch aus LaTeX heraus aktualisieren?

# Bibliotheken und Hilfsfunktionen laden --------------------------------------
library("fst")
library("stringr")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/printf.r")


# Konfiguration ---------------------------------------------------------------
sources <- c("Bitfinex", "Bitstamp", "Coinbase", "Kraken", "TrueFX", "Dukascopy")
filterByPairs <- c("btcusd", "btceur", "eurusd")


# Tabelle erzeugen ------------------------------------------------------------
`%nin%` = Negate(`%in%`)
for (source in sources) {
    pairs <- list.files(sprintf("Cache/%s", tolower(source)))
    for (pair in pairs) {
        
        if (pair %nin% filterByPairs) {
            next
        }
        
        files <- list.files(
            sprintf("Cache/%s/%s/tick", source, pair),
            full.names=TRUE
        )
        
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
        
        source_formatted <- str_replace(source, "Coinbase", "Coinbase Pro")
        printf("        %s &\n", source_formatted) # Börse / Datenquelle
        printf("            %s &\n", format.currencyPair(pair)) # Kurspaar
        printf("            %s &\n", format.number(numTicks)) # Anzahl Ticks
        printf("            %s &\n", format(firstDataset, "%d.%m.%Y %H:%M:%S")) # Von
        printf("            %s \\\\\n", format(lastDataset, "%d.%m.%Y %H:%M:%S")) # Bis
        printf("\n")
    }
}
