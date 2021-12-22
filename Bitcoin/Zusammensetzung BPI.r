(function() {
    
    # Konfiguration -----------------------------------------------------------
    asTeX = FALSE
    texFile = "/Users/fox/Documents/Studium - Promotion/TeX/Grafiken/Bitcoin_Datensatz_Zusammensetzung_BPI.tex"
    
    
    # Lese Daten --------------------------------------------------------------
    bpi = read_csv(
        "Daten/coindesk/BPI Zusammensetzung.csv",
        cols(.default=col_character()),
        col_names = TRUE
    )
    
    # Entferne Notizen
    #bpi$Notes = NULL
    
    # Umwandlungen. Zeitzone: UTC
    bpi = as.data.frame(bpi)
    bpi$TimeFrom = anytime(bpi$TimeFrom, asUTC=TRUE)
    bpi$TimeTo = anytime(bpi$TimeTo, asUTC=TRUE)
    
    # Setze aktuell noch gültige Bestandteile auf heute
    bpi$TimeTo[bpi$TimeTo > Sys.time()] = Sys.time()
    
    # Sortiere - hier von Hand. ggplot sortiert anhand der levels. Beachte das "rev".
    bpi$Exchange = factor(bpi$Exchange, levels = rev(c(
        "Mt. Gox", "CampBX", "BTC-e", "Bitstamp", "Bitfinex", "LakeBTC",
        "OkCoin", "itBit", "Coinbase"
    )))
    setorder(bpi, Exchange)
    #bpi = bpi[order(bpi$Exchange),]
    

    # Grafiken ----------------------------------------------------------------
    if (asTeX) {
        cat("Ausgabe als TeX.\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = defaultImageHeight,
            sanitize = TRUE
        )
    }
    
    plot = bpi %>%
        ggplot(aes(x=TimeFrom, y=Exchange, colour=Exchange)) +
        geom_errorbarh(
            aes(xmin=TimeFrom, xmax=TimeTo, height=0),
            show.legend=FALSE,
            size=5
        ) +
        theme_minimal() +
        theme(panel.grid.major.x=element_line(colour=rgb(0,0,0,.4))) +
        scale_colour_ptol() +
        scale_x_datetime(date_breaks="1 year", date_minor_breaks="1 month", date_labels="%Y") +
        labs(
            #title="Zusammensetzung des coindesk.com Bitcoin Price Index (USD)",
            #subtitle=paste0("Quelle: coindesk.com (Stand ", format(Sys.Date(), format="%d. %B %Y"), ")"),
            x="Datum",
            y="B\\\"orse"
        )
    print(plot)
    if (asTeX) {
        dev.off()
    }
    
    bpi <<- bpi
    
})()
