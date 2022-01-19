#' Veranschauliche beispielhaft Ausreißer und Kurssprünge
#' in verschiedenen Bitcoin-Sets.


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
library("fst")
library("data.table") # %between%
library("ggplot2")
library("ggthemes")
source("Konfiguration/FilePaths.r")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/printf.r")


# Konfiguration ---------------------------------------------------------------
plotAsLaTeX <- TRUE
texFile_a <- sprintf(
    "%s/Abbildungen/Bitcoin_Preischarakteristik_BitstampUSDAusreisser.tex",
    latexOutPath
)
texFile_b <- sprintf(
    "%s/Abbildungen/Bitcoin_Preischarakteristik_KrakenUSDAusreisser.tex",
    latexOutPath
)
texFile_c <- sprintf(
    "%s/Abbildungen/Bitcoin_Preischarakteristik_CoinbaseProUSDAusreisser.tex",
    latexOutPath
)


# BTC/USD an Bitstamp, 10.10.2021 zwischen 20:35 und 20:36 UTC ----------
a_datalimits <- c("2021-10-10 20:35:20", "2021-10-10 20:35:45")
a_viewport <- as.POSIXct(c("2021-10-10 20:35:23", "2021-10-10 20:35:41"))
a_bitstamp <- read_fst(
    "Cache/bitstamp/btcusd/tick/bitstamp-btcusd-tick-2021-10.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
a_bitstamp <- a_bitstamp[Time %between% a_datalimits]
a_bitstamp[,Exchange:=factor("Bitstamp", levels=c("Bitstamp", "Coinbase Pro"))]

# Bei Wahl einer Liniengrafik müsste Bitstamp nach Zeit UND Preis sortiert werden,
# da mehrere Ticks in einer Sekunde auftreten.
#setorder(a_bitstamp, Time, Price)

# Zum Vergleich wird die Börse Coinbase Pro dargestellt.
a_coinbase <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2021-10.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
a_coinbase <- a_coinbase[Time %between% a_datalimits]
a_coinbase[,Exchange:=factor("Coinbase Pro", levels=c("Bitstamp", "Coinbase Pro"))]

if (plotAsLaTeX) {
    plotTitle <- NULL
    plotXLab <- "\\footnotesize Uhrzeit"
    plotYLab <- "\\footnotesize Preis in USD"
    
    # Bibliotheken/Konfiguration laden
    source("Konfiguration/TikZ.r")
    printf.debug("Ausgabe als LaTeX in Datei %s\n", texFile_a)
    tikz(
        file = texFile_a,
        width = documentPageWidth,
        height = 5 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
    
} else {
    plotTitle <- "BTC/USD am 10.11.2021 um 20:35 UTC"
    plotXLab <- "Uhrzeit"
    plotYLab <- "Preis in USD"
}

print(
    ggplot() +
        geom_line(
            aes(x=Time, y=Price, color=Exchange, linetype=Exchange),
            data=a_coinbase,
            size=.75
        ) +
        # geom_point(
        #     aes(x=Time, y=Price, color=Exchange, linetype=Exchange),
        #     # Warnmeldung: "Ignoring unknown aesthetics: linetype" ist
        #     # *nicht* zutreffend, ohne die Angabe von `linetype` wird eine
        #     # weitere Legende eingezeichnet!
        #     data=a_bitstamp,
        #     size=1,
        #     shape=4
        # ) +
        geom_boxplot(
            aes(x=Time, group=Time, y=Price, color=Exchange, linetype=Exchange),
            data=a_bitstamp,
            width=.5,
            outlier.size=.75,
            # Boxplot nicht in der Legende kennzeichen
            show.legend=FALSE
        ) +
        theme_minimal() +
        theme(
            #legend.position = "none",
            legend.position = c(0.85, 0.3),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(), #element_text(size=9),
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            date_breaks="5 sec",
            date_minor_breaks="1 sec",
            date_labels="%H:%M:%S",
            expand = expansion(mult = c(.03, .03))
        ) +
        scale_y_continuous(
            labels = function(x) format.money(x, digits=0)
        ) +
        # Sichtbaren Bereich begrenzen, ohne dass Daten abgeschnitten werden
        coord_cartesian(xlim=a_viewport) +
        scale_color_ptol() +
        guides(
            
        ) +
        labs(
            title=plotTitle,
            x=plotXLab,
            y=plotYLab
        )
)

if (plotAsLaTeX) {
    dev.off()
}




# BTC/USD an Kraken, 22.02.2021 zwischen 14:16 und 14:30 UTC ------------------
b_kraken <- read_fst(
    "Cache/kraken/btcusd/tick/kraken-btcusd-tick-2021-02.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
b_kraken <- b_kraken[Time %between% c("2021-02-22 14:16:00", "2021-02-22 14:30:00")]
b_kraken[,Exchange:="Kraken"]

# Zum Vergleich wird die Börse Coinbase Pro auf 5s-Basis dargestellt.
b_coinbase <- read_fst(
    "Cache/coinbase/btcusd/5s/coinbase-btcusd-5s-2021-02.fst",
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
        height = 5 / 2.54, # cm -> Zoll
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
            legend.position = c(0.85, 0.3),
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


# BTC/USD an Coinbase Pro, 14.01.2015 zwischen 06:00 und 18:00 UTC ------------
c_datalimits <- c("2015-01-14 03:00:00", "2015-01-14 22:00:00")
c_viewport <- as.POSIXct(c("2015-01-14 06:00:00", "2015-01-14 18:00:00"))
c_coinbase <- read_fst(
    "Cache/coinbase/btcusd/tick/coinbase-btcusd-tick-2015-01.fst",
    columns=c("Time","Price"),
    as.data.table=T
)
c_coinbase <- c_coinbase[Time %between% c_datalimits]
c_coinbase[,Exchange:="Coinbase Pro"]

# Zum Vergleich wird die Börse Bitstamp auf 60s-Basis dargestellt.
c_bitstamp <- read_fst(
    "Cache/bitstamp/btcusd/60s/bitstamp-btcusd-60s-2015-01.fst",
    columns=c("Time","Close"),
    as.data.table=T
)
c_bitstamp <- c_bitstamp[Time %between% c_datalimits]
c_bitstamp[,Exchange:="Bitstamp"]
setnames(c_bitstamp, "Close", "Price")

# Datensatz kombinieren
c_combined <- rbindlist(list(c_coinbase, c_bitstamp))

# Sortieren und Kraken zuerst anzeigen
setorder(c_combined, Time)
c_combined[,Exchange:=factor(Exchange, levels=c("Coinbase Pro", "Bitstamp"))]

if (plotAsLaTeX) {
    plotTitle <- NULL
    plotXLab <- "\\footnotesize Uhrzeit"
    plotYLab <- "\\footnotesize Preis in USD"
    printf.debug("Ausgabe als LaTeX in Datei %s\n", texFile_c)
    tikz(
        file = texFile_c,
        width = documentPageWidth,
        height = 5 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
    
} else {
    plotTitle <- "BTC/USD am 14.01.2015 zwischen 06:00 und 18:00 (UTC)"
    plotXLab <- "Uhrzeit"
    plotYLab <- "Preis in USD"
}

print(
    ggplot(c_combined, aes(x=Time, y=Price)) +
        # `size` unverändert lassen, andernfalls sind die feinen Linien
        # dieses Ausschnittes nur schwer erkennbar
        geom_line(aes(color=Exchange, linetype=Exchange)) + 
        theme_minimal() +
        theme(
            legend.position = c(0.85, 0.3),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(), #element_text(size=9),
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            #date_breaks="2 mins",
            #date_minor_breaks="1 minute",
            date_labels="%H:%M",
            expand = expansion(mult = c(.01, .03))
        ) +
        scale_y_continuous(
            labels = function(x) format.money(x, digits=0)
        ) +
        # Sichtbaren Bereich begrenzen, ohne dass Daten abgeschnitten werden
        coord_cartesian(xlim=c_viewport) +
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

