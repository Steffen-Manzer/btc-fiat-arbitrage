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


# BTC/GBP an Kraken zwischen dem 08. und 15.01.2018 ===========================
a_kraken <- read_fst(
    "Cache/kraken/btcgbp/tick/kraken-btcgbp-tick-2018-01.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
a_kraken <- a_kraken[Time %between% c("2018-01-08", "2018-01-15")]
a_kraken[,Exchange:="Kraken"]

# Zum Vergleich wird die Börse Coinbase Pro auf 60s-Basis dargestellt.
a_coinbase <- read_fst(
    "Cache/coinbase/btcgbp/60s/coinbase-btcgbp-60s-2018-01.fst",
    columns=c("Time","Close"),
    as.data.table=T
)
a_coinbase <- a_coinbase[Time %between% c("2018-01-08", "2018-01-15")]
a_coinbase[,Exchange:="Coinbase Pro"]
setnames(a_coinbase, "Close", "Price")

# Datensatz kombinieren
a_combined <- rbindlist(list(a_kraken, a_coinbase))

# Sortieren und Kraken zuerst anzeigen
setorder(a_combined, Time)
a_combined[,Exchange:=factor(Exchange, levels=c("Kraken", "Coinbase Pro"))]

if (plotAsLaTeX) {
    plotTitle <- NULL
    plotXLab <- "\\footnotesize Datum"
    plotYLab <- "\\footnotesize Preis in GBP"
    
    # Bibliotheken/Konfiguration laden
    source("Konfiguration/TikZ.r")
    printf.debug("Ausgabe als LaTeX in Datei %s\n", texFile_a)
    tikz(
        file = texFile_a,
        width = documentPageWidth,
        #height = 6 / 2.54, # cm -> Zoll
        height = 7 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
    
} else {
    plotTitle <- "BTC/GBP zwischen 08. und 15.01.2018"
    plotXLab <- "Datum"
    plotYLab <- "Preis in GBP"
}

print(
    ggplot(a_combined, aes(x=Time, y=Price)) +
        geom_line(aes(color=Exchange, linetype=Exchange), size=1) +
        theme_minimal() +
        theme(
            #legend.position = "none",
            legend.position = c(0.2, 0.85),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(), #element_text(size=9),
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            date_breaks="1 day",
            #date_minor_breaks="12 hours",
            date_labels="%d.%m.",
            expand = expansion(mult = c(.03, .03))
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


# BTC/USD an Kraken, 22.02.2021 zwischen 14:16 und 14:30 UTC ==================
b_kraken <- read_fst(
    "Cache/kraken/btcusd/tick/kraken-btcusd-tick-2021-02.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
b_kraken <- b_kraken[Time %between% c("2021-02-22 14:16:00", "2021-02-22 14:30:00")]
b_kraken[,Exchange:="Kraken"]

# Zum Vergleich wird die Börse Coinbase Pro auf 1s-Basis dargestellt.
b_coinbase <- read_fst(
    "Cache/coinbase/btcusd/1s/coinbase-btcusd-1s-2021-02.fst",
    columns=c("Time","Close"),
    as.data.table=T
)
b_coinbase <- b_coinbase[Time %between% c("2021-02-22 14:16:00", "2021-02-22 14:30:00")]
b_coinbase[,Exchange:="Coinbase Pro"]
setnames(b_coinbase, "Close", "Price")

# Datensatz kombinieren
b_combined <- rbindlist(list(b_kraken, b_coinbase))

# Sortieren und Kraken zuerst anzeigen
setorder(b_combined, Time)
b_combined[,Exchange:=factor(Exchange, levels=c("Kraken", "Coinbase Pro"))]

if (plotAsLaTeX) {
    plotTitle <- NULL
    plotXLab <- "\\footnotesize Uhrzeit"
    plotYLab <- "\\footnotesize Preis in USD"
    printf.debug("Ausgabe als LaTeX in Datei %s\n", texFile_b)
    tikz(
        file = texFile_b,
        width = documentPageWidth,
        #height = 6 / 2.54, # cm -> Zoll
        height = 7 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
    
} else {
    plotTitle <- "BTC/USD am 22.02.2021 zwischen 14:16 und 14:30 UTC"
    plotXLab <- "Uhrzeit"
    plotYLab <- "Preis in USD"
}

print(
    ggplot(b_combined, aes(x=Time, y=Price)) +
        # `size` unverändert lassen, andernfalls sind die feinen Linien
        # dieses Ausschnittes nur schwer erkennbar
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
            date_breaks="2 mins",
            #date_minor_breaks="1 minute",
            date_labels="%H:%M",
            expand = expansion(mult = c(.01, .03))
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
