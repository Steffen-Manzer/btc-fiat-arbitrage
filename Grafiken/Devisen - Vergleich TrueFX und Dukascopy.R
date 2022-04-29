#' Berechne/Zeige Übereinstimmung respektive Unterschiede der Preisen und
#' des Spread für TrueFX- und Dukascopy-Tickdaten, um zu überprüfen, ob beide
#' Datensätze ähnlich genug sind, um sie für eine Analyse der Bitcoin-Daten
#' verwenden zu können.


# Bibliotheken, Hilfsfunktionen und Konfiguration laden -----------------------
library("fst")
library("data.table")
library("lubridate")
library("ggplot2")
library("ggthemes")
library("tictoc")
source("Funktionen/AddOneMonth.R")
source("Funktionen/AppendToDataTable.R")
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")


# Konfiguration und Variablen -------------------------------------------------

# Startzeit
# Beginn der vorliegenden Daten: 01.01.2020
# Beginn der Bitcoin-Daten: 18.08.2011
# Dreiecksarbitrage: Frühestens ab Oktober 2013
now <- as.POSIXct("2013-01-01")

# Ausgabedateien
plotAsLaTeX <- TRUE
texFileSpread <- sprintf(
    "%s/Abbildungen/Empirie_Devisen_TrueFX_Dukascopy_Spread.tex",
    latexOutPath
)
texFilePricedifferences <- sprintf(
    "%s/Abbildungen/Empirie_Devisen_TrueFX_Dukascopy_Preisunterschied.tex",
    latexOutPath
)

# Tickdaten einlesen und Statistiken für jeden Monat berechnen ----------------
if (file.exists("Cache/stats/forex-comparison-spread.fst")) {
    # Daten bereits berechnet
    spread <- read_fst(
        "Cache/stats/forex-comparison-spread.fst", 
        as.data.table=TRUE
    )
    differences <- read_fst(
        "Cache/stats/forex-comparison-differences.fst", 
        as.data.table=TRUE
    )
    median_per_day <- read_fst(
        "Cache/stats/forex-comparison-median_per_day.fst", 
        as.data.table=TRUE
    )
} else {
    # Daten neu berechnen. Dauer: Ca. 5 Minuten
    spread <- data.table()
    differences <- data.table()
    median_per_day <- data.table()
    while (TRUE) {
        tic()
        printf("Lese %d-%02d...", year(now), month(now))
        file <- format(now, "%Y-%m")
        
        # Quelldateien bestimmen
        sourceFileTrueFX <- sprintf(
            "Cache/truefx/eurusd/tick/truefx-eurusd-tick-%d-%02d.fst",
            year(now), month(now)
        )
        sourceFileDukascopy <- sprintf(
            "Cache/dukascopy/eurusd/tick/dukascopy-eurusd-tick-%d-%02d.fst",
            year(now), month(now)
        )
        
        # Daten lesen
        truefx <- read_fst(
            sourceFileTrueFX, columns=c("Time", "Bid", "Ask"), 
            as.data.table = TRUE
        )
        dukascopy <- read_fst(
            sourceFileDukascopy, columns=c("Time", "Bid", "Ask"), 
            as.data.table = TRUE
        )
        
        # Spread
        truefx.spread <- summary(truefx$Ask - truefx$Bid)
        spread <- appendDT(spread, list(
            Time = as.double(now),
            Source = "TrueFX",
            Min = truefx.spread[[1]],
            Q1 = truefx.spread[[2]],
            Median = truefx.spread[[3]],
            Mean = truefx.spread[[4]],
            Q3 = truefx.spread[[5]],
            Max = truefx.spread[[6]]
        ))

        dukascopy.spread <- summary(dukascopy$Ask - dukascopy$Bid)
        spread <- appendDT(spread, list(
            Time = as.double(now),
            Source = "Dukascopy",
            Min = dukascopy.spread[[1]],
            Q1 = dukascopy.spread[[2]],
            Median = dukascopy.spread[[3]],
            Mean = dukascopy.spread[[4]],
            Q3 = dukascopy.spread[[5]],
            Max = dukascopy.spread[[6]]
        ))
        
        
        # Abweichung des Mittelkurses auf Sekundenbasis
        truefx[,Mittel:=(Bid+Ask)/2]
        truefx <- truefx[
            j=.(Mittel=last(Mittel)),
            by=.(Time=floor_date(Time, unit="seconds"))
        ]
        dukascopy[,Mittel:=(Bid+Ask)/2]
        dukascopy <- dukascopy[
            j=.(Mittel=last(Mittel)),
            by=.(Time=floor_date(Time, unit="seconds"))
        ]
        
        # Nur Daten betrachten, die in beiden Datensätzen vorkommen
        truefx <- truefx[Time %in% dukascopy$Time]
        dukascopy <- dukascopy[Time %in% truefx$Time]
        
        # Q1, Median, Mean, Q3
        mittel_diff <- summary(abs(truefx$Mittel - dukascopy$Mittel))
        differences <- appendDT(differences, list(
            Time = as.double(now),
            Min = mittel_diff[[1]],
            Q1 = mittel_diff[[2]],
            Median = mittel_diff[[3]],
            Mean = mittel_diff[[4]],
            Q3 = mittel_diff[[5]],
            Max = mittel_diff[[6]]
        ))
        
        
        # Median der Abweichungen auf Tagesbasis (derzeit nicht genutzt)
        truefx_dukascopy_differences <- data.table(
            Time = truefx$Time,
            Difference = abs(truefx$Mittel - dukascopy$Mittel)
        )
        truefx_dukascopy_differences <- truefx_dukascopy_differences[
            j=.(MedianDifference=median(Difference)),
            by=.(Time=floor_date(Time, unit="1 day"))
        ]
        median_per_day <- rbindlist(list(
            median_per_day,
            truefx_dukascopy_differences
        ))
        
        toc()
        
        # Nächster Monat
        now <- addOneMonth(now)
        
        # Vor aktuellem Monat abbrechen
        if (now >= Sys.Date() - 30) {
            break
        }
    }
    rm(dukascopy, truefx)
    gc()
    
    spread <- cleanupDT(spread)
    spread[,Source:=factor(Source, levels=c("TrueFX", "Dukascopy"))]
    spread[,Time:=as.POSIXct(Time, origin="1970-01-01", tz="UTC")]

    differences <- cleanupDT(differences)
    differences[,Time:=as.POSIXct(Time, origin="1970-01-01", tz="UTC")]
    
    if (!dir.exists("Cache/stats")) {
        dir.create("Cache/stats", recursive=TRUE)
    }
    write_fst(spread, "Cache/stats/forex-comparison-spread.fst", compress=100L)
    write_fst(differences, "Cache/stats/forex-comparison-differences.fst", compress=100L)
    write_fst(median_per_day, "Cache/stats/forex-comparison-median_per_day.fst", compress=100L)
}


# Grafisch darstellen ---------------------------------------------------------

# Spread
if (plotAsLaTeX) {
    source("Konfiguration/TikZ.R")
    cat("Ausgabe in Datei", texFileSpread, "\n")
    tikz(
        file = texFileSpread,
        width = documentPageWidth,
        height = 5 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}
print(
    ggplot(spread, aes(x=Time)) +
        geom_line(aes(y=Median * 1e4, color=Source, linetype=Source), size=1) +
        theme_minimal() +
        theme(
            legend.position = c(0.85, 0.85),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 12, 5, 5),
            legend.title = element_blank(), #element_text(size=9),
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            date_breaks="1 year",
            date_labels="%Y",
            expand = expansion(mult = c(.01, .03))
        ) +
        scale_y_continuous(
            breaks=seq(from=0, to=1.4, by=.2),
            limits=c(0, 1.400001)
        ) +
        scale_color_ptol() + 
        labs(
            x="\\footnotesize Datum",
            y="\\footnotesize Spread [USD-Pip]"
        )
)

if (plotAsLaTeX) {
    dev.off()
}

# Preisunterschiede
if (plotAsLaTeX) {
    cat("Ausgabe in Datei", texFilePricedifferences, "\n")
    tikz(
        file = texFilePricedifferences,
        width = documentPageWidth,
        height = 5 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}
print(
    ggplot(differences, aes(x=Time)) +
        geom_ribbon(aes(ymin=Q1*1e4, ymax=Q3*1e4), alpha=.3) +
        geom_line(aes(y=Median * 1e4, color="Difference"), size=1) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            date_breaks="1 year",
            date_labels="%Y",
            expand = expansion(mult = c(.01, .03))
        ) +
        scale_y_continuous(
            #breaks=seq(from=0, to=20, by=2),
            #limits=c(0, 20.00001)
        ) +
        scale_color_ptol() + 
        labs(
            x="\\footnotesize Datum",
            y="\\footnotesize Unterschied [USD-Pip]"
        )
)

if (plotAsLaTeX) {
    dev.off()
}

# Preisunterschiede als Tages-Median: Nicht genutzt
# print(
#     ggplot(median_per_day, aes(x=Time)) +
#         geom_line(aes(y=MedianDifference * 1e4, color="Difference"), size=1) +
#         theme_minimal() +
#         theme(
#             legend.position = "none",
#             axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
#             axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
#         ) +
#         scale_x_datetime(
#             date_breaks="1 year",
#             #date_minor_breaks="1 sec",
#             date_labels="%Y",
#             expand = expansion(mult = c(.01, .03))
#         ) +
#         scale_y_continuous(
#             #breaks=seq(from=0, to=20, by=2),
#             #limits=c(0, 20.00001)
#         ) +
#         scale_color_ptol() + 
#         labs(
#             x="\\footnotesize Datum",
#             y="\\footnotesize Median des Preisunterschiedes in Pip [USD]"
#         )
# )

