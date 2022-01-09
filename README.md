# Arbitrage zwischen Bitcoin- und Devisenmärkten

Crawler, Aggregatoren und R-Skripte zur Analyse von Arbitragemöglichkeiten zwischen
Bitcoin- und Devisenmärkten.

![Raumarbitrage BTC/USD](https://research.noecho.de/Logo@2x.png)

## Versionshinweise

- Die Auswertungsskripte wurden unter R 4.1.2 entwickelt.
- Die in PHP geschriebenen Daten-Crawler wurden unter PHP 8.1 entwickelt.
- Die in Python geschriebenen Daten-Crawler wurden unter Python 3.9 entwickelt.

Abweichende Versionen, insbesondere ältere Versionen, können funktionieren, wurden jedoch
nicht getestet.

## Verbindung zu LaTeX
Für eine automatische Datenaktualisierung aus LaTeX heraus wird in einigen Skripten der
LaTeX-Befehl `executeR` erwähnt:

```latex
% R aus LaTeX heraus ausführen
\newcommand{\executeR}[1]{%
    \ignorespaces%
    \immediate%
    \write18{%
        echo;%
        echo "executeR: #1:";%
        cd "/Pfad/zu/den/R/Quelldateien";%
        R --no-save --no-restore --slave -f '#1' --args FromLaTeX;%
        echo;%
    }%
    \ignorespaces%
}
```

Bei Kompilierung der LaTeX-Dokumente (mit der Option -shell-escape) wird das entsprechende
R-Skript aufgerufen und aktualisiert Abbildungen, Tabellen oder Statistiken, die
wieder in LaTeX eingebunden sein können.
