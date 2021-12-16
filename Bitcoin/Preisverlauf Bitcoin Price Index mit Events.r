(function() {
    
    # Derzeit nicht genutzt und nicht fertiggestellt.
    warning("TODO Events einlesen")
    return()
    
    
    ####
    # Aktualisierung:
    # - coindesk-bpi-close.csv.gz aktualisieren
    # - 2-1 BPI Rohdaten zu 60s rds.r ausführen
    # - events-relevant.csv und Dateiname aktualisieren von 99bitcoins.com
    # - Dieses Skript ausführen (ggf. mit asTeX = FALSE prüfen)
    ####
    
    
    # Konfiguration -----------------------------------------------------------
    asTeX = FALSE
    texFile = "/Users/fox/Documents/Studium - Promotion/TeX/Grafiken/BPI_Log_Historisch_mit_Events.tex"
    doPlot = TRUE # Ansonsten nur Daten in den globalen Variablenraum einlesen
    bpiSource = "Cache/coindesk/bpi-daily-btcusd.rds"
    
    #eventsSource = "Daten/Bitcoin-Events/events-20180808.csv"
    eventsSource = "Cache/99bitcoins/events.csv"
    
    timeframeSource = "Daten/Bitcoin-Events/Ausgewählte Zeiträume.csv"
    
    limitGraphToTime = c("2011-01-01", "2030-01-01") # Daten beginnen ab Mitte 2010
    
    # Daten aufbereiten -------------------------------------------------------
    
    # BPI
    bpi.perDay = readRDS(bpiSource)
    bpi.perDay = bpi.perDay[
        bpi.perDay$Time >= limitGraphToTime[1] & 
        bpi.perDay$Time <  limitGraphToTime[2],
    ]
    bpi.perDay$Time = as.POSIXct(bpi.perDay$Time) # Wir brauchen POSIXct für datetime-Achse...
    bpi.perDay <<- bpi.perDay
    
    
    # Ereignisse
    events = read_csv(
        eventsSource,
        col_names = TRUE,
        cols(
            timestamp = col_integer(),
            time = col_character(),
            title = col_character()
        )
    )
    events = as.data.table(events)
    
    events$Time = anytime(events$timestamp)
    events$timestamp = NULL
    events = events[
        events$Time >= limitGraphToTime[1] &
        events$Time <  limitGraphToTime[2],
    ]
    
    bitcoinEvents <<- events
    
    
    # Ausgewählte Zeiträume
    timeframesRaw = read_csv(
        timeframeSource,
        col_types = cols(
            TimeFrom = col_datetime(format = "%Y-%m-%d"), 
            TimeTo = col_datetime(format = "%Y-%m-%d")
        )
    )
    timeframesRaw = as.data.table(timeframesRaw)
    
    timeframes = list()
    i = 0
    for (category in unique(timeframesRaw$Category)) {
        i = i + 1
        currentFrame = timeframesRaw[Category==category]
        thisFrame = data.table()
        
        # Starte beim zweiten Datensatz und markiere nur Beginn
        for (row in 2:nrow(currentFrame)) {
            currentTimespan = currentFrame[row]
            thisFrame = rbind(thisFrame, data.table(
                Description = category,
                Time = currentTimespan$TimeFrom
            ))
        }
        
        timeframes[[i]] = thisFrame
    }
    
    timeframes <<- timeframes
    
    
    
    if (!doPlot) {
        return()
    }
    
    # Grafiken erstellen ------------------------------------------------------
    chart.title = "Bitcoin Price Index - Ereignisse"
    chart.subtitle=paste0(
        "Quelle: coindesk.com (",
        format(first(bpi.perDay$Time), format="%d.%m.%Y bis "),
        format(last(bpi.perDay$Time), format="%d.%m.%Y"),
        ")"
    )
    chart.x = "Datum"
    chart.y = "Bitcoin-Preisindex"
    if (asTeX) {
        cat("Ausgabe in Datei ", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = defaultImageHeight,
            sanitize = TRUE
        )
        chart.title = NULL
    }
    
    # Linke Zeitachse
    chart.xLimLeft = as.POSIXct(paste0(year(min(bpi.perDay$Time)), "-01-01"))
    if (month(max(bpi.perDay$Time)) > 6) {
        # Rechte Zeitachse wenn über Juli: Anfang nächstes Jahr
        # Sonst wird Jahr trotzdem angezeigt und abgeschnitten dargestellt
        chart.xLimRight = as.POSIXct(paste0(year(max(bpi.perDay$Time)) + 1, "-01-01"))
    } else {
        # Rechte Zeitachse automatisch bis Juni
        chart.xLimRight = NA
    }
    
    for (timeframe in timeframes) {
        chart.title = paste0("Bitcoin Price Index - ", timeframe[1]$Description)
        chart.subtitle=paste0(
            "Quelle: coindesk.com (",
            format(first(bpi.perDay$Time), format="%d.%m.%Y bis "),
            format(last(bpi.perDay$Time), format="%d.%m.%Y"),
            ")"
        )
        chart.x = "Datum"
        chart.y = "Bitcoin-Preisindex"
        
        # Linke Zeitachse
        chart.xLimLeft = as.POSIXct(paste0(year(min(bpi.perDay$Time)), "-01-01"))
        if (month(max(bpi.perDay$Time)) > 6) {
            # Rechte Zeitachse wenn über Juli: Anfang nächstes Jahr
            # Sonst wird Jahr trotzdem angezeigt und abgeschnitten dargestellt
            chart.xLimRight = as.POSIXct(paste0(year(max(bpi.perDay$Time)) + 1, "-01-01"))
        } else {
            # Rechte Zeitachse automatisch bis Juni
            chart.xLimRight = NA
        }
        
        # Erstelle Plot mit Tagesdaten
        plot = bpi.perDay %>%
            ggplot(aes(x=Time)) +
            geom_vline(data=timeframe, aes(xintercept=Time), color="red", size=.5) +
            geom_line(aes(y=Close), size=.5) +
            theme_minimal() +
            theme(
                axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
                axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
            ) +
            scale_fill_ptol() +
            scale_x_datetime(
                limits = c(chart.xLimLeft, chart.xLimRight),
                date_breaks="1 year",
                minor_breaks=NULL,
                date_labels="%Y"
            ) +
            scale_y_log10(
                labels = function(x) { paste(prettyNum(x, big.mark=".", decimal.mark=","), "USD") },
                breaks = c(0.1, 1, 10, 100, 1000, 10000),
                minor_breaks = NULL
            ) + 
            labs(
                title=chart.title,
                subtitle=chart.subtitle,
                x=chart.x,
                y=chart.y
            )
        
        
        print(plot)
    }
    
    
})()
