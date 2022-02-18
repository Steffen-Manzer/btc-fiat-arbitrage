# Benötigte Bibliotheken laden
library("data.table") # rbindlist, setorder
library("zoo") # rollapply

#' Zwei Datensätze in eine gemeinsame Liste zusammenführen
#' 
#' Verbindet zwei Sätze von Tickdaten in eine gemeinsame Liste,
#' sortiert diese nach Zeit und entfernt die mittleren von
#' drei oder mehr aufeinanderfolgenden Ticks der selben Börse, 
#' da diese für die Auswertung nicht relevant sind.
#' 
#' @param dataset_a `data.table` die mindestens die
#'                  Spalten `Time` und `Exchange` enthält
#' @param dataset_b Wie `dataset_a`.
#' @return `data.table` Eine Tabelle der Tickdaten beider Börsen
mergeSortAndFilterTwoDatasets <- function(dataset_a, dataset_b) {
    
    # Merge, Sort und Filter (nicht: mergesort-Algorithmus)
    # Daten zu einer gemeinsamen Liste verbinden
    dataset_ab <- rbindlist(
        list(dataset_a, dataset_b),
        use.names = TRUE
    )
    
    # Liste nach Zeit sortieren
    setorder(dataset_ab, Time)
    
    # `dataset_ab` enthält nun Ticks beider Börsen nach Zeit sortiert.
    # Aufeinanderfolgende Daten der selben Börse interessieren nicht,
    # da der Tickpunkt davor bzw. danach immer näher am nächsten Tick 
    # der anderen Börse ist. Aufeinanderfolgende Tripel daher
    # herausfiltern, dies beschleunigt die weitere Verarbeitung
    # signifikant (etwa um den Faktor 5).
    #
    # Beispiel:
    # |--------------------------------->   Zeitachse
    #  A A A A B B A B B B A A B B B B A    Ticks der Börsen A und B
    #         *   * *     *   *       *     Sinnvolle Preisvergleiche
    #  * * *           *         * *        Nicht benötigte Ticks
    #        A B B A B   B A A B     B A    Reduzierter Datensatz
    
    # Tripel filtern
    # Einschränkung: Erste und letzte Zeile werden nie entfernt,
    # diese können an dieser Stelle nicht sinnvoll geprüft werden.
    # Im weiteren Verlauf wird erneut auf die gleiche Börse 
    # aufeinanderfolgender Ticks geprüft.
    triplets <- c(
        FALSE, # Ersten Tick immer beibehalten
        rollapply(
            dataset_ab$Exchange,
            width = 3,
            # Es handelt sich um einen zu entfernenden Datenpunkt,
            # wenn die Börse im vorherigen, aktuellen und nächsten 
            # Tick identisch ist.
            FUN = function(exchg) all(exchg == exchg[1])
        ),
        FALSE # Letzten Tick immer beibehalten
    )
    
    # Datensatz ohne gefundene Tripel zurückgeben
    return(dataset_ab[!triplets])
}
