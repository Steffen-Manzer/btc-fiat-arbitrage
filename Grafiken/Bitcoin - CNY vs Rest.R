# Keine dynamische Aktualisierung via LaTeX, da statisches Zeitfenster

library("data.table")
library("anytime")
library("dplyr")
library("ggplot2")
library("ggthemes")

(function() {
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Krypto_Handelsvolumen_BTCCNY_vs_Rest.tex", latexOutPath)
    dataSource <- "http://data.bitcoinity.org/export_data.csv?c=c&data_type=volume&t=b&timespan=all"
    plotAsLaTeX <- FALSE
    
    # Daten aufbereiten
    btcVolume <- fread(dataSource)
    
    btcVolume$Time <- anydate(btcVolume$Time)
    
    # Erste Zeile enthält immer fehlerhafte Daten
    btcVolume <- btcVolume[btcVolume$Time > "2010-01-01"]
    
    # CNY herausnehmen und Rest aggregieren
    cnyVolume <- btcVolume$CNY
    btcVolume$CNY <- NULL
    otherVolume <- rowSums(btcVolume[,2:ncol(btcVolume)])
    btcVolume <- data.table(Time = btcVolume$Time, CNY = cnyVolume, Andere = otherVolume)
    
    # Fehlende Daten auf 0 setzen
    btcVolume[is.na(btcVolume)] <- 0
    
    # Melt
    
    # In melt(., id.vars = "Time", variable.name = "Currency", value.name = "Volume") :
    # The melt generic in data.table has been passed a grouped_df and will attempt to redirect to
    # the relevant reshape2 method; please note that reshape2 is deprecated, and this redirection
    # is now deprecated as well. To continue using melt methods from reshape2 while both libraries
    # are attached, e.g. melt.list, you can prepend the namespace like reshape2::melt(.).
    # In the next version, this warning will become an error.
    
    # TODO Ggf. mittels data.table neu schreiben
    btcVolume <- btcVolume %>%
        group_by(Time) %>%
        reshape2::melt(id.vars = "Time", variable.name="Currency", value.name="Volume")
    btcVolume$Time <- anydate(btcVolume$Time)
    
    # Zeitraum begrenzen
    btcVolume <- btcVolume[btcVolume$Time >= "2014-01-01" & btcVolume$Time <= "2018-01-01"]
    
    # Grafiken erstellen
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = defaultImageHeight * .7,
            sanitize = TRUE
        )
    }
    
    plot <- btcVolume %>%
        ggplot(aes(x=Time)) +
        geom_line(aes(y=Volume/1e6, color=Currency, linetype=Currency), linewidth=1) +
        scale_color_ptol() +
        scale_fill_ptol() +
        scale_x_date(
            date_breaks="1 year",
            date_labels="%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_continuous(
            labels=function(x) { paste(prettyNum(x, big.mark=".", decimal.mark=",")) }
        ) + 
        theme_minimal() +
        theme(
            legend.position = c(0.12, 0.75),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(), #element_text(size=9),
            axis.title.x = element_text(size = 9, margin = margin(t = 10)),
            axis.title.y = element_text(size = 9, margin = margin(r = 10))
        ) +
        labs(x="Datum", y="Monatsumsatz [Mio. BTC]")
    print(plot)
    if (plotAsLaTeX) {
        dev.off()
    }
    
    
})()
