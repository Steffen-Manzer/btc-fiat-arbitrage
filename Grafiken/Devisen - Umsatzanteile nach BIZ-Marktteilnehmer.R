#' Verwendet in Arbeit:
#' Kapitel Währungen und Devisen - Devisenmärkte - Wichtige Währungen und Wechselkurse
#' Label: ?
#'
#' Aktualisierung:
#' - Daten von BIZ für 2019 laden und in CSV exportieren
#' - Dieses Skript ausführen (ggf. mit asTeX = FALSE prüfen)


# Bibliotheken laden ----------------------------------------------------------
source("Konfiguration/FilePaths.R")
library("data.table")
library("ggplot2")
library("ggthemes")


# Konfiguration ---------------------------------------------------------------
asTeX <- F
texFile <- sprintf("%s/Abbildungen/Devisen_Umsatz_nach_Marktteilnehmer.tex",
                   latexOutPath)


# Daten aufbereiten -----------------------------------------------------------
counterparties <- fread(
    "Daten/Devisen Umsatz nach Marktteilnehmer/Umsatz_Marktteilnehmer.csv",
    colClasses = c("character", "character", "double", "integer")
)
counterparties$Jahr <- as.Date(counterparties$Jahr, format = "%Y")

# Richtige Sortierung der Kategorien
counterparties[,Kategorie:=factor(Kategorie, levels=unique(Kategorie))]
#counterparties$Kategorie <- factor(counterparties$Kategorie, levels=unique(counterparties$Kategorie))


# Grafiken erstellen ----------------------------------------------------------
if (asTeX) {
    source("Konfiguration/TikZ.R")
    cat("Ausgabe in Datei ", texFile, "\n")
    tikz(
        file = texFile,
        width = documentPageWidth,
        height = 5.7 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}

plot <- ggplot(counterparties, aes(x=Jahr, y=Umsatz)) +
    geom_line(aes(color=Kategorie, linetype=Kategorie), linewidth=1) +
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

