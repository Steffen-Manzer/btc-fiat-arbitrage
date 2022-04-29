#' Erzeugt eine grafische Übersicht über die Anzahl gefundener
#' (theoretischer) Arbitragemöglichkeiten in Abhängigkeit
#' des gewählten Schwellwertes für ein Tauschgeschäft.


# Funktionen und Bibliotheken laden -------------------------------------------
source("Funktionen/FormatCurrencyPair.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")
library("fst")
library("data.table")
library("stringr") # str_split
library("ggplot2")
library("ggthemes")
library("gridExtra") # grid.arrange


# Konfiguration ---------------------------------------------------------------
plotAsLaTeX <- TRUE
texFile <- sprintf(
    "%s/Abbildungen/Dreiecksarbitrage/GrundgesamtheitNachThreshold.tex",
    latexOutPath
)

exchangeNames <- list(
    "bitfinex" = "Bitfinex",
    "bitstamp" = "Bitstamp",
    "coinbase" = "Coinbase Pro",
    "kraken" = "Kraken"
)

# Übersicht einlesen ----------------------------------------------------------
metadata <- data.table()

# Mögliche zeitliche Limits durchgehen
for (t in c(1, 2, 5, 10)) {
    
    # Börsenpaare durchgehen
    # ACHTUNG: Annahme, dass es nur eine Teildatei gibt!
    files <- list.files(
        sprintf("Cache/Dreiecksarbitrage/%ds/", t),
        pattern = ".+-1\\.fst",
        full.names = TRUE
    )
    for (f in files) {
        
        # Informationen aus Dateinamen auslesen:
        # bitfinex-usd-eur-1.fst
        n <- basename(f)
        parts <- n |> str_split(fixed("-"))
        exchange <- parts[[1]][1]
        currency_a <- parts[[1]][2] |> toupper()
        currency_b <- parts[[1]][3] |> toupper()
        numRows <- metadata_fst(f)$nrOfRows
        
        metadata <- rbindlist(list(
            metadata,
            data.table(
                threshold = t,
                exchange = exchangeNames[[exchange]],
                currency_a = currency_a,
                currency_b = currency_b,
                currencyPair = sprintf("%s/%s", currency_a, currency_b),
                numRows = numRows
            )
        ))
        
        # printf("%s,%s,%s,%s,%d\n", 
        #        t, exchange_a, exchange_b, currencyPair, numRows)
    }
}


# Plot erzeugen ---------------------------------------------------------------

# Absolut als Barchart
metadata[,thresholdFactor:=factor(threshold, levels=c("1", "2", "5", "10"))]
p <-
    ggplot(metadata, aes(x=exchange, y=numRows, fill=thresholdFactor)) +
    geom_bar(
        position = position_dodge(width=.92),
        stat = "identity",
        width = .75
    ) +
    geom_text(
        aes(label=format.numberWithFixedDigits(numRows/1e6, digits=1L)),
        position = position_dodge(width=.92),
        vjust = -0.5,
        size = 3
    ) +
    scale_x_discrete(expand=c(.12,.12)) +
    scale_y_continuous(
        labels = function(x) format.number(x/1e6),
        limits = c(0, 45e6)
    ) +
    scale_fill_ptol(labels=function(x) paste0(x, "\\,s")) +
    theme_minimal() +
    theme(
        axis.title.x = element_text(size = 9, margin = margin(t = 10)),
        axis.title.y = element_text(size = 9, margin = margin(r = 10)),
        legend.title = element_text(size = 9)
    ) +
    labs(x="Börse", y="Tauschmöglichkeiten [Mio.]", fill="Grenzwert")

if (plotAsLaTeX) {
    source("Konfiguration/TikZ.R")
    printf("Ausgabe in Datei %s\n", texFile)
    tikz(
        file = texFile,
        width = documentPageWidth,
        height = 6 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}

print(p)

if (plotAsLaTeX) {
    dev.off()
}


# Statistiken ausgeben ----------------------------------------------------

for (exchangeName in unique(metadata$exchange)) {
    printf("%s:\n", exchangeName)
    
    result <- metadata[exchange == exchangeName & threshold == 10L, numRows] /
        metadata[exchange == exchangeName & threshold == 1L, numRows]
    printf("    1s -> 10s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[exchange == exchangeName & threshold == 2L, numRows] /
        metadata[exchange == exchangeName & threshold == 1L, numRows]
    printf("    1s ->  2s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[exchange == exchangeName & threshold == 5L, numRows] /
        metadata[exchange == exchangeName & threshold == 2L, numRows]
    printf("    2s ->  5s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[exchange == exchangeName & threshold == 10L, numRows] /
        metadata[exchange == exchangeName & threshold == 5L, numRows]
    printf("    5s -> 10s: %s %%\n", format.percentage(result - 1, 1L))
    
}
