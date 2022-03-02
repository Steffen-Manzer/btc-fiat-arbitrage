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
    "%s/Abbildungen/Raumarbitrage/GrundgesamtheitNachThreshold.tex",
    latexOutPath
)

exchangeNames <- list(
    "bitfinex" = "Bitfinex",
    "bitstamp" = "Bitstamp",
    "coinbase" = "Coinbase Pro",
    "kraken" = "Kraken"
)
metadata <- data.table()

# Übersicht einlesen ----------------------------------------------------------
# Mögliche zeitliche Limits durchgehen
for (t in c(2, 5, 10, 30)) {
    
    # Börsenpaare durchgehen
    files <- list.files(
        sprintf("Cache/Raumarbitrage %ds/", t),
        pattern = ".+-1\\.fst",
        full.names = TRUE
    )
    for (f in files) {
        
        # Informationen aus Dateinamen auslesen:
        # btcgbp-bitstamp-coinbase-1.fst
        n <- basename(f)
        parts <- n |> str_split(fixed("-"))
        currencyPair <- parts[[1]][1]
        exchange_a <- parts[[1]][2]
        exchange_b <- parts[[1]][3]
        numRows <- metadata_fst(f)$nrOfRows
        
        metadata <- rbindlist(list(
            metadata,
            data.table(
                threshold = as.character(t),
                exchange_a = exchange_a,
                exchange_b = exchange_b,
                exchangePair = sprintf(
                    "%s - %s",
                    exchangeNames[[exchange_a]],
                    exchangeNames[[exchange_b]]
                ),
                currencyPair = currencyPair,
                numRows = numRows
            )
        ))
        
        # printf("%s,%s,%s,%s,%d\n", 
        #        t, exchange_a, exchange_b, currencyPair, numRows)
    }
}


# Daten aufbereiten -----------------------------------------------------------

# Kurspaare nicht alphabetisch, sondern nach Bedeutung sortieren
metadata[,currencyPair:=factor(currencyPair, levels = c("btcusd", "btceur", "btcgbp", "btcjpy"))]

# Prozentuale Anteile berechnen
# Minimum = Basis = 2s - Alles weitere kommt "on top" dazu
metadata[, additionalRows := numRows - metadata[threshold=="2s"]$numRows]
metadata[threshold=="2s", additionalRows := numRows]


# Plot erzeugen ---------------------------------------------------------------

# Absolut als zwei Barcharts mit separaten Achseneinteilungen
metadata[,threshold:=factor(threshold, levels=c("2", "5", "10", "30"))]
p1 <- ggplot(metadata[currencyPair != "btcjpy"]) +
    geom_bar(
        aes(x=currencyPair, y=numRows, fill=threshold),
        position = position_dodge(width=.85),
        stat = "identity",
        width = .75
    ) +
    scale_x_discrete(labels=format.currencyPair, expand=c(.15,.15)) +
    scale_y_continuous(
        breaks = seq(0, 70e6, by=10e6),
        labels = function(x) format.number(x/1e6),
        limits = c(0, 70e6)
    ) +
    scale_fill_ptol(labels=function(x) paste0(x, "\\,s")) +
    theme_minimal() +
    theme(
        legend.position = "none",
        axis.title.x = element_text(size = 9, margin = margin(t = 10)),
        axis.title.y = element_text(size = 9, margin = margin(r = 10))
    ) +
    labs(x="Kurspaar", y="Tauschmöglichkeiten [Mio.]")
p2 <- ggplot(metadata[currencyPair == "btcjpy"]) +
    geom_bar(
        aes(x=currencyPair, y=numRows, fill=threshold),
        position = position_dodge(width=.85),
        stat = "identity",
        width = .75
    ) +
    scale_x_discrete(labels=format.currencyPair, expand=c(.15,.15)) +
    scale_y_continuous(
        breaks = seq(0, 175e3, by=25e3),
        labels = function(x) format.number(x/1e3),
        limits = c(0, 175e3)
    ) +
    scale_fill_ptol(labels=function(x) paste0(x, "\\,s")) +
    theme_minimal() +
    theme(
        axis.title.x = element_text(size = 9, margin = margin(t = 10)),
        axis.title.y = element_text(size = 9, margin = margin(r = 10)),
        legend.title = element_text(size = 9)
    ) +
    labs(x="Kurspaar", y="Tauschmöglichkeiten [Tsd.]", fill="Schwellwert")

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

grid.arrange(
    p1, p2,
    layout_matrix = rbind(c(1,2)),
    widths = c(9,6)
)

if (plotAsLaTeX) {
    dev.off()
}

