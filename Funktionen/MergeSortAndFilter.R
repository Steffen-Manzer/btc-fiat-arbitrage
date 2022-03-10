# Benötigte Bibliotheken laden
library("data.table") # rbindlist, setorder
library("zoo") # rollapply

#' Zwei Datensätze in eine gemeinsame Liste zusammenführen
#' 
#' Verbindet zwei Sätze von Tickdaten in eine gemeinsame Liste,
#' sortiert diese nach Zeit und entfernt alle mittleren von
#' drei oder mehr aufeinanderfolgenden Ticks der selben Börse oder
#' des selben Wechselkurses (`compare_by`).
#' 
#' Beispiel Raumarbitrage: Vergleich anhand der Börse:
#' |--------------------------------->   Zeitachse
#'  A A A A B B A B B B A A B B B B A    Ticks der Börsen A oder B
#'         *   * *     *   *       *     Sinnvolle Preisvergleiche
#'  ? * *           *         * *        Nicht benötigte Ticks
#'  A     A B B A B   B A A B     B A    Reduzierter Datensatz
#' 
#' @param dataset_a `data.table` mit `Time` und `compare_by` (s.u.)
#' @param dataset_b Wie `dataset_a`.
#' @param compare_by Spalte anhand derer doppelte Ticks erkannt werden
#' @return `data.table` Eine Tabelle der Tickdaten beider Börsen
mergeSortAndFilterTwoDatasets <- function(
    dataset_a,
    dataset_b,
    compare_by = "Exchange"
) {
    # Daten zu einer gemeinsamen Liste verbinden, nach Zeit sortieren
    dataset_ab <- rbindlist(list(dataset_a, dataset_b))
    setorder(dataset_ab, Time)
    
    # Tripel heraussuchen
    triplets <- c(
        FALSE, # Ersten Tick immer beibehalten
        rollapply(
            data = dataset_ab[[compare_by]],
            # Es handelt sich um einen zu entfernenden Datenpunkt,
            # wenn der Vergleichswert im vorherigen, aktuellen und 
            # nächsten Tick identisch ist. Daher: Fensterbreite 3
            width = 3,
            FUN = function(x) all(x == x[1])
        ),
        FALSE # Letzten Tick immer beibehalten
    )
    
    # Datensatz ohne die identifizierten Tripel zurückgeben
    return(dataset_ab[!triplets])
}
