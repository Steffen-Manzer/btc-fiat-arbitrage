#' Ein Bitcoin- oder Wechselkursdatensatz.
#'
#' Definiert als eigene Klasse, die per Referenz übergeben und somit
#' bei Bedarf in-place bearbeitet werden kann.
library("data.table")
setRefClass(
    "Dataset", 
    fields=list(
    
        # Börse (Bitcoin) oder Datenquelle (Devisen) in Kleinbuchstaben,
        # z.B. `bitfinex` oder `dukascopy`.
        Exchange = "character",
        
        # Kurspaar in Kleinbuchstaben, z.B. `btcusd` oder `eurusd`.
        CurrencyPair = "character",
        
        # Absoluter Pfad zu den aufbereiteten Daten, z.B.
        # `/home/.../R/Cache/bitfinex/btcusd/tick/bitfinex-btcusd-tick`
        #
        # An diesen Pfad wird `-{JAHR}-{MONAT}.fst` angehängt, um die jeweiligen
        # Daten einzulesen.
        PathPrefix = "character",
        
        # Letzter verfügbarer Monat
        EndDate = "POSIXct",
        
        # Eingelesene Daten
        data = "data.table",
        
        # Zeiträume mit Ausreißern. Nur nötig, falls diese beim
        # Laden neuer Daten ausgelassen werden sollen.
        SuspiciousPeriods = "data.table",
        
        # Handelsstunden eines Devisendatensatzes (z.B. BTC/USD)
        # Genutzt, um Bitcoin-Daten für Dreiecksarbitrage auf
        # gemeinsame Zeiträume zu begrenzen
        TradingHours = "POSIXct"
        
    )
)
