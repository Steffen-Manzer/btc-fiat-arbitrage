#' Zwei Datensätze auf den gemeinsamen Zeitraum beschränken
#' 
#' Beide Datensätze starten aufgrund der Art und Weise,
#' wie Daten geladen werden, immer ungefähr zum gleichen Zeitpunkt.
#' Es muss also nur ein Enddatum bestimmt werden.
#' 
#' @param dataset_a Eine Instanz der Klasse `Dataset`
#' @param dataset_b Eine Instanz der Klasse `Dataset`
#' @return `NULL` (Verändert die angegebenen Datensätze per Referenz.)
filterTwoDatasetsByCommonTimeInterval <- function(dataset_a, dataset_b)
{
    # Parameter und Daten validieren
    stopifnot(
        inherits(dataset_a, "Dataset"), nrow(dataset_a$data) > 0L, 
        inherits(dataset_b, "Dataset"), nrow(dataset_b$data) > 0L
    )
    
    # Letzte Tick-Zeitpunkte bestimmen
    last_a <- last(dataset_a$data$Time)
    last_b <- last(dataset_b$data$Time)
    
    # Beide Datensätze enden am exakt gleichen Zeitpunkt.
    # In diesem Fall muss nichts gefiltert werden.
    if (last_a == last_b) {
        return(invisible(NULL))
    }
    
    # Behalte nur gemeinsame Daten.
    # Ergänze Datensatz um einen weiteren Datenpunkt für letzten Vergleich.
    # `data.table` kann leider noch kein subsetting per Referenz, sodass eine
    # Kopie (`<-`) notwendig ist.
    if (last_a > last_b) {
        
        # Datensatz A enthält mehr Daten als B
        pos_of_last_common_tick <- last(dataset_a$data[Time <= last_b, which=TRUE])
        dataset_a$data <- dataset_a$data[1L:(pos_of_last_common_tick+1L)]
        
    } else {
        
        # Datensatz B enthält mehr Daten als A
        pos_of_last_common_tick <- last(dataset_b$data[Time <= last_a, which=TRUE])
        dataset_b$data <- dataset_b$data[1L:(pos_of_last_common_tick+1L)]
        
    }
    
    # Ein resultierender Datensatz ist leer
    if (nrow(dataset_a$data) == 0L || nrow(dataset_b$data) == 0L) {
        stop("filterTwoDatasetsByCommonTimeInterval: ein Datensatz ist nach dem Filtern leer!\n")
    }
    
    return(invisible(NULL))
}
