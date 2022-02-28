# Benötigte Bibliotheken laden
library("data.table") # rbindlist, setorder
library("zoo") # rollapply

#' Zwei Datensätze in eine gemeinsame Liste zusammenführen
#' 
#' Verbindet zwei Sätze von Tickdaten in eine gemeinsame Liste,
#' sortiert diese nach Zeit und entfernt alle mittleren von
#' drei oder mehr aufeinanderfolgenden Ticks der selben Börse.
#' 
#' @param dataset_a `data.table` mit den Spalten `Time` und `Exchange`
#' @param dataset_b Wie `dataset_a`.
#' @return `data.table` Eine Tabelle der Tickdaten beider Börsen
mergeSortAndFilterTwoDatasets <- function(dataset_a, dataset_b)
{
    # Daten zu einer gemeinsamen Liste verbinden, nach Zeit sortieren
    dataset_ab <- rbindlist(list(dataset_a, dataset_b))
    setorder(dataset_ab, Time)
    
    # Tripel heraussuchen. Einschränkung: Erste und letzte Zeile
    # werden nie entfernt, diese können hier nicht geprüft werden.
    # Beispiel:
    # |--------------------------------->   Zeitachse
    #  A A A A B B A B B B A A B B B B A    Ticks der Börsen A und B
    #         *   * *     *   *       *     Sinnvolle Preisvergleiche
    #  ? * *           *         * *        Nicht benötigte Ticks
    #  A     A B B A B   B A A B     B A    Reduzierter Datensatz
    triplets <- c(
        FALSE, # Ersten Tick immer beibehalten
        rollapply(
            data = dataset_ab$Exchange,
            # Es handelt sich um einen zu entfernenden Datenpunkt,
            # wenn die Börse im vorherigen, aktuellen und nächsten 
            # Tick identisch ist. Daher Fensterbreite: 3 Ticks.
            width = 3,
            FUN = function(exchg) all(exchg == exchg[1])
        ),
        FALSE # Letzten Tick immer beibehalten
    )
    
    # Datensatz ohne die identifizierten Tripel zurückgeben
    return(dataset_ab[!triplets])
}
