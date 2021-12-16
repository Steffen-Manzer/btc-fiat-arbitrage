
(function() {
    
    ####
    # Verwendet in Arbeit:
    # Kapitel Währungen und Devisen - Devisenmärkte - Wichtige Währungen und Wechselkurse
    # Label: ?
    #
    # Aktualisierung:
    # - Daten von BIZ für 2019 laden und in CSV exportieren
    # - Dieses Skript ausführen (ggf. mit asTeX = FALSE prüfen)
    ####
    
    
    # Konfiguration -----------------------------------------------------------
    asTeX <- F
    texFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Devisen_Umsatz_nach_Marktteilnehmer.tex"
    
    # Bibliotheken laden ------------------------------------------------------
    library("data.table")
    library("dplyr")
    library("ggplot2")
    library("ggthemes")
    
    # Daten aufbereiten -------------------------------------------------------
    counterparties <- fread(
        "Daten/Devisen Umsatz nach Marktteilnehmer/Umsatz_Marktteilnehmer.csv",
        colClasses = c("character", "character", "double", "integer")
    )
    
    counterparties$Jahr <- as.Date(counterparties$Jahr, format = "%Y")
    
    # Richtige Sortierung
    counterparties$Kategorie <- factor(counterparties$Kategorie, levels = unique(counterparties$Kategorie))
    
    
    # Grafiken erstellen ------------------------------------------------------
    if (asTeX) {
        source("Konfiguration/TikZ.r")
        cat("Ausgabe in Datei ", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 5.7 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    plot <- counterparties %>%
        ggplot(aes(x=Jahr, y=Umsatz)) +
        geom_line(aes(color=Kategorie, linetype=Kategorie), size=1) +
        scale_x_date(
            labels = function(x) { format(x, "%Y") },
            breaks = counterparties$Jahr,
            minor_breaks = NULL,
            expand = expansion(mult = c(.04, .04))
        ) +
        scale_y_continuous(
            labels = function(x) { paste(prettyNum(x, big.mark=".", decimal.mark=",")) },
            breaks = seq(0, 3e3, 1e3),
            limits = c(0, max(counterparties$Umsatz)),
            expand = expansion(mult = c(0, .05))
        ) + 
        scale_color_ptol() + 
        theme_minimal() +
        theme(
            legend.position = c(0.22, 0.8),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 15, 5, 5),
            legend.title = element_blank(),
            axis.title.x = element_text(size = 9, margin = margin(t = 10)),
            axis.title.y = element_text(size = 9, margin = margin(r = 10))
        ) +
        labs(x="Berichtsjahr", y="Tagesumsatz [Mrd. USD]")
    
    print(plot)
    if (asTeX) {
        dev.off()
    }
    
})()
