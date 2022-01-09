#' Veranschauliche beispielhaft Lücken und Kurssprünge
#' in verschiedenen Bitcoin-Sets.


# Bibliotheken und externe Hilfsfunktionen laden ==============================
library("fst")
library("data.table") # %between%
library("ggplot2")
library("ggthemes")
source("Konfiguration/FilePaths.r")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/printf.r")


# Konfiguration ===============================================================
plotAsLaTeX <- TRUE
texFile_a <- sprintf(
    "%s/Abbildungen/Bitcoin_Preischarakteristik_KrakenGBPAusreisser.tex",
    latexOutPath
)
texFile_b <- sprintf(
    "%s/Abbildungen/Bitcoin_Preischarakteristik_KrakenUSDAusreisser.tex",
    latexOutPath
)


# BTC/GBP an der Börse Kraken zwischen dem 08. und 15.01.2018 =================
a_kraken <- read_fst(
    "Cache/kraken/btcgbp/tick/kraken-btcgbp-tick-2018-01.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
a_kraken <- a_kraken[Time %between% c("2018-01-08", "2018-01-15"),]

if (plotAsLaTeX) {
    plotTitle <- NULL
    plotXLab <- "\\footnotesize Datum"
    plotYLab <- "\\footnotesize Preis in GBP"
    
    # Bibliotheken/Konfiguration laden
    source("Konfiguration/TikZ.r")
    printf.debug("Ausgabe als LaTeX in Datei %s\n", texFile)
    tikz(
        file = texFile_a,
        width = documentPageWidth,
        #height = 6 / 2.54, # cm -> Zoll
        height = 7 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
    
} else {
    plotTitle <- "BTC/GBP an der Börse Kraken zwischen 08. und 15.01.2018"
    plotXLab <- "Datum"
    plotYLab <- "Preis in GBP"
}

print(
    ggplot(a_kraken, aes(x=Time, y=Price)) +
        geom_line(aes(color="Kraken")) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            date_breaks="1 day",
            #date_minor_breaks="12 hours",
            date_labels="%d.%m.%Y",
            expand = expansion(mult = c(.05, .06)) # Enddatum vollständig anzeigen
        ) +
        scale_y_continuous(
            labels = function(x) format.money(x, digits=0)
        ) +
        scale_color_ptol() +
        labs(
            title=plotTitle,
            x=plotXLab,
            y=plotYLab
        )
)

if (plotAsLaTeX) {
    dev.off()
}


# BTC/USD an der Börse Kraken am 22.02.2021 zwischen 14:15 und 14:32 UTC ======
b_kraken <- read_fst(
    "Cache/kraken/btcusd/tick/kraken-btcusd-tick-2021-02.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
b_kraken <- b_kraken[Time %between% c("2021-02-22 14:15:00", "2021-02-22 14:32:00")]
b_kraken[,Exchange:="Kraken"]

# Zum Vergleich wird die Börse Coinbase Pro auf 1s-Basis dargestellt.
b_coinbase <- read_fst(
    "Cache/coinbase/btcusd/1s/coinbase-btcusd-1s-2021-02.fst",
    columns=c("Time","Close"),
    as.data.table=T
)
b_coinbase <- b_coinbase[Time %between% c("2021-02-22 14:15:00", "2021-02-22 14:32:00")]
b_coinbase[,Exchange:="Coinbase Pro"]
setnames(b_coinbase, "Close", "Price")

# Datensatz kombinieren
b_combined <- rbindlist(list(b_kraken, b_coinbase))

# Sortieren und Kraken zuerst anzeigen
setorder(b_combined, Time)
b_combined[,Exchange:=factor(Exchange, levels=c("Kraken", "Coinbase Pro"))]

if (plotAsLaTeX) {
    plotTitle <- NULL
    plotXLab <- "\\footnotesize Datum"
    plotYLab <- "\\footnotesize Preis in USD"
    printf.debug("Ausgabe als LaTeX in Datei %s\n", texFile)
    tikz(
        file = texFile_b,
        width = documentPageWidth,
        #height = 6 / 2.54, # cm -> Zoll
        height = 7 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
    
} else {
    plotTitle <- "BTC/USD am 22.02.2021 zwischen 14:15 und 14:32 UTC"
    plotXLab <- "Datum"
    plotYLab <- "Preis in USD"
}

print(
    ggplot(b_combined, aes(x=Time, y=Price)) +
        geom_line(aes(color=Exchange, linetype=Exchange)) +
        theme_minimal() +
        theme(
            legend.position = c(0.85, 0.2),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(), #element_text(size=9),
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            #date_breaks="5 minutes",
            #date_minor_breaks="1 minute",
            date_labels="%H:%M:%S",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_continuous(
            labels = function(x) format.money(x, digits=0)
        ) +
        scale_color_ptol() +
        labs(
            title=plotTitle,
            x=plotXLab,
            y=plotYLab
        )
)

if (plotAsLaTeX) {
    dev.off()
}
