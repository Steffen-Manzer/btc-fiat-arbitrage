# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    stop("Derzeit nicht genutzt.")
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Bitcoin_Energieverbrauch.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Abbildungen/Bitcoin_Energieverbrauch_Stand.tex", latexOutPath)
    asTeX <- fromLaTeX || FALSE
    
    
    # Berechnungen ------------------------------------------------------------
    # Jahresenergiebedarf Bitcoin
    # https://static.dwcdn.net/data/cFnri.csv
    # via https://datawrapper.dwcdn.net/cFnri/8/
    #   via https://digiconomist.net/bitcoin-energy-consumption/
    apiSourceFile <- "https://static.dwcdn.net/data/cFnri.csv"
    
    # Stromerzeugung Deutschland
    # https://www-genesis.destatis.de/genesis/online?operation=table&code=43312-0001&bypass=true&levelindex=1&levelid=1629705181020
    comparisonApiGermany <- "https://www-genesis.destatis.de/genesisWS/rest/2020/data/table"
    #comparisonApiGermanyParams <- "name=43312-0001&startyear=2018&transpose=true&username=DEJ07P56OT&password=K7QN@RF8YwFWnph798_k"
    comparisonApiGermanyParams <- "name=43311-0002&startyear=2016&username=DEJ07P56OT&password=K7QN@RF8YwFWnph798_k"
    
    # Nur einmal pro Woche neu laden
    if (
        fromLaTeX && asTeX && file.exists(texFile) && 
        difftime(Sys.time(), file.mtime(texFile), units = "days") < 8
    ) {
        cat("Grafik Bitcoin-Energieverbrauch noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    
    # Bibliotheken und Hilfsfunktionen laden ----------------------------------
    source("Funktionen/R_in_LaTeX_Warning.R")
    library("data.table")
    #library("rjson") # Für Destatis
    #library("stringr") # Destatis-Datum einlesen
    #library("purrr") # Destatis-Datum einlesen
    library("lubridate") # floor_date
    #library("dplyr") # Nur für auskommentierte Bereiche nötig
    library("ggplot2")
    library("ggthemes")
    
    # Daten einlesen: Bitcoin
    rawData <- fread(
        apiSourceFile,
        header = TRUE,
        colClasses = c("character", "numeric", "numeric"),
        col.names = c("Time", "Geschätzter Jahresenergiebedarf", "Minimaler Jahresenergiebedarf")
    )
    cat(nrow(rawData), "Datensaetze geladen.\n")
    rawData[, Time := as.Date(parse_date_time2(rawData$Time, order = "Y/m/d", tz="UTC"))]
    rawData <- melt(
        rawData,
        id.vars = "Time",
        variable.name = "Type",
        value.name = "Energiemenge_in_TWh"
    )
    
    
    # Daten einlesen: Destatis
    # failed <- FALSE
    # tryCatch(
    #     {
    #         apiResponse <- system2("curl", c(
    #             '-H "Accept: application/json"',
    #             paste0('-d "', comparisonApiGermanyParams, '"'),
    #             '-G',
    #             comparisonApiGermany
    #         ), stdout = TRUE, timeout = 60)
    #         
    #         if (nchar(apiResponse) < 10) {
    #             simpleError("Invalid response.")
    #         }
    #         
    #         apiResponse <- do.call(paste0, as.list(apiResponse))
    #         apiResponse <- fromJSON(apiResponse)
    #         
    #         # Prüfe Validität
    #         if (length(apiResponse) != 5) {
    #             simpleError("Invalid response length.")
    #         }
    #         if (apiResponse$Status$Code > 0) {
    #             simpleError(apiResponse$Status$Content)
    #         }
    #         
    #         destatisData <<- apiResponse$Object$Content
    #         
    #     },
    #     error = function(err) {
    #         failed <<- TRUE
    #         warning(err)
    #     }
    # )
    # 
    # if (failed) {
    #     cat(latexWarning("Konnte Destatis-Daten nicht lesen!"), file = outFile)
    #     return()
    # }
    # 
    # # Datensatz aufbereiten
    # destatisTable <- fread(destatisData, skip = 7, fill = TRUE, select=c(1,2,3,5))
    # #destatisTable <- destatisTable_Total #%>%
    #     #select(1, 2, 3, 4, ncol(destatisTable_Total))
    # colnames(destatisTable) <- c("Year", "Month", "Type", "Value")
    # #Value = Elektrizitätserzeugung (netto) in MWh
    # 
    # # Letzte drei Zeilen entfernen
    # destatisTable <- head(destatisTable, -3)
    # 
    # # Werte einlesen
    # destatisTable$Value <- as.numeric(destatisTable$Value)
    # 
    # # Nicht benötigte Daten entfernen und korrekt bezeichnen
    # destatisTable <- destatisTable %>%
    #     filter(Type == "Insgesamt" & Value != "...")
    # destatisTable$Type = "Stromerzeugung"
    # 
    # # Datum einlesen...
    # destatisTable$Month <- reduce2(
    #     c("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"), 
    #     c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"), 
    #     .init=destatisTable$Month,
    #     str_replace
    # )
    # destatisTable$Time <- as.Date(
    #     parse_date_time2(paste0(destatisTable$Year, "-", destatisTable$Month, "-01"), order = "Y-m-d", tz="UTC")
    # )
    # destatisTable$Year <- NULL
    # destatisTable$Month <- NULL
    # destatisTable <- destatisTable %>%
    #     relocate(Time) %>%
    #     arrange(Time)
    # 
    # # Annualisieren
    # destatisTable$Annualized <- frollsum(destatisTable$Value, n = 12)
    # 
    # # Auf gemeinsame Daten beschränken
    # destatisTable <- destatisTable[destatisTable$Time >= rawData$Time[1]]
    
    
    # Grafiken erstellen
    if (asTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    plot <- ggplot(rawData, aes(x=Time)) +
        geom_line(
            aes(y=Energiemenge_in_TWh, color=Type, linetype=Type),
            size=1
        ) +
        theme_minimal() +
        theme(
            legend.title = element_blank(),
            legend.position = "bottom",
            legend.margin = margin(t=-5),
            legend.text = element_text(margin = margin(r = 15)),
            axis.title.x = element_text(size = 9, margin = margin(t = 10)),
            axis.title.y = element_text(size = 9, margin = margin(r = 10))
        ) +
        scale_fill_ptol() +
        scale_x_date(
            date_breaks="1 year",
            minor_breaks=NULL,
            date_labels="%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_continuous(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            expand = expansion(mult = c(.02, .02))
        ) +
        expand_limits(y = 0) + # 0 mit enthalten
        scale_color_ptol() +
        labs(x="Datum", y="Energiemenge [TWh]") ; print(plot)
    
    if (asTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
})()
