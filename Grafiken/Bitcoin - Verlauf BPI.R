# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    ####
    # Verwendet in Arbeit:
    # Kapitel 3.1.2.2 Entwicklung und Marktdurchdringung dezentraler Systeme
    # Label: Grafik:Bitcoin_Preise_Historisch_BPI_Log
    ####
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Krypto_Bitcoin_Preis_BPI.tex", latexOutPath)
    outFileTimestamp <- sprintf(
        "%s/Abbildungen/Krypto_Bitcoin_Preis_BPI_Stand.tex",
        latexOutPath
    )
    plotAsLaTeX <- fromLaTeX || TRUE
    
    # Nur einmal pro Monat neu laden
    if (
        fromLaTeX && plotAsLaTeX && file.exists(texFile) && 
        difftime(Sys.time(), file.mtime(texFile), units = "days") < 28
    ) {
        cat("Grafik BTCUSD noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    # Bibliotheken laden ------------------------------------------------------
    library("fst")
    library("data.table")
    library("ggplot2")
    library("ggthemes")
    
    # Berechnungen durchführen ------------------------------------------------
    # Quelldaten einlesen
    btcusd <- read_fst("Cache/coindesk/bpi-daily-btcusd.fst", as.data.table = TRUE)
    
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei ", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 7 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    # Nutze Tagesdaten
    plot <- ggplot(btcusd, aes(x=Time)) +
        geom_line(aes(y=Close, color="BTCUSD"), linewidth=1) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10), size=9),
            axis.title.y = element_text(margin = margin(r = 10), size=9)
        ) +
        scale_x_date(
            date_breaks="1 year",
            minor_breaks=NULL,
            date_labels="%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_log10(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            breaks = c(0.1, 1, 10, 100, 1000, 10000),
            minor_breaks = NULL
        ) + 
        scale_color_ptol() +
        labs(x="Datum", y="Bitcoin-Preis [USD]")
    
    print(plot)
    if (plotAsLaTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(last(btcusd$Time), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
})()
