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

# Übersicht einlesen ----------------------------------------------------------
metadata <- data.table()

# Mögliche zeitliche Limits durchgehen
for (t in c(2, 5, 10, 30)) {
    
    # Börsenpaare durchgehen
    # ACHTUNG: Annahme, dass es nur eine Teildatei gibt!
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
                threshold = t,
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

# Börsenpaare nach Währung gruppieren
metadata <- metadata[
    j = .(numRows = sum(numRows)),
    by = .(threshold, currencyPair)
]

# Kurspaare nicht alphabetisch, sondern nach ihrer Bedeutung sortieren
metadata[,currencyPair:=factor(currencyPair, levels = c("btcusd", "btceur", "btcgbp", "btcjpy"))]


# Plot erzeugen ---------------------------------------------------------------

# Absolut als zwei Barcharts mit separaten Achseneinteilungen
metadata[,thresholdFactor:=factor(threshold, levels=c("2", "5", "10", "30"))]
p1 <- 
    ggplot(
        metadata[currencyPair != "btcjpy"],
        aes(x=currencyPair, y=numRows, fill=thresholdFactor)
    ) +
    geom_bar(
        position = position_dodge(width=.85),
        stat = "identity",
        width = .75
    ) +
    geom_text(
        aes(label=round(numRows/1e6)),
        position = position_dodge(width=.85),
        vjust = -0.5,
        size = 3
    ) +
    scale_x_discrete(labels=format.currencyPair, expand=c(.15,.15)) +
    scale_y_continuous(
        breaks = seq(0, 225e6, by=50e6),
        labels = function(x) format.number(x/1e6),
        limits = c(0, 235e6)
    ) +
    scale_fill_ptol(labels=function(x) paste0(x, "\\,s")) +
    theme_minimal() +
    theme(
        legend.position = "none",
        axis.title.x = element_text(size = 9, margin = margin(t = 10)),
        axis.title.y = element_text(size = 9, margin = margin(r = 10))
    ) +
    labs(x="Kurspaar", y="Tauschmöglichkeiten [Mio.]")
p2 <- 
    ggplot(
        metadata[currencyPair == "btcjpy"],
        aes(x=currencyPair, y=numRows, fill=thresholdFactor)
    ) +
    geom_bar(
        position = position_dodge(width=.85),
        stat = "identity",
        width = .75
    ) +
    geom_text(
        aes(label=round(numRows/1e3)),
        position = position_dodge(width=.85),
        vjust = -0.5,
        size = 3
    ) +
    scale_x_discrete(labels=format.currencyPair, expand=c(.15,.15)) +
    scale_y_continuous(
        breaks = seq(0, 225e3, by=50e3),
        labels = function(x) format.number(x/1e3),
        limits = c(0, 235e3)
    ) +
    scale_fill_ptol(labels=function(x) paste0(x, "\\,s")) +
    theme_minimal() +
    theme(
        axis.title.x = element_text(size = 9, margin = margin(t = 10)),
        axis.title.y = element_text(size = 9, margin = margin(r = 10)),
        legend.title = element_text(size = 9)
    ) +
    labs(x="Kurspaar", y="Tauschmöglichkeiten [Tsd.]", fill="Grenzwert")

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


# Statistiken ausgeben ----------------------------------------------------

for (pair in unique(metadata$currencyPair)) {
    
    printf("%s:\n", format.currencyPair(pair))
    
    result <- metadata[currencyPair == pair & threshold == 30L, numRows] /
        metadata[currencyPair == pair & threshold == 2L, numRows]
    printf("     2s -> 30s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[currencyPair == pair & threshold == 30L, numRows] /
        metadata[currencyPair == pair & threshold == 5L, numRows]
    printf("     5s -> 30s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[currencyPair == pair & threshold == 5L, numRows] /
        metadata[currencyPair == pair & threshold == 2L, numRows]
    printf("     2s ->  5s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[currencyPair == pair & threshold == 10L, numRows] /
        metadata[currencyPair == pair & threshold == 5L, numRows]
    printf("     5s -> 10s: %s %%\n", format.percentage(result - 1, 1L))
    
    result <- metadata[currencyPair == pair & threshold == 30L, numRows] /
        metadata[currencyPair == pair & threshold == 10L, numRows]
    printf("    10s -> 30s: %s %%\n", format.percentage(result - 1, 1L))
    
}
