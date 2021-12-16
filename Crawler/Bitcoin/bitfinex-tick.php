<?php

// Geschrieben für PHP 8.1 (CLI oder HTTP)

require_once '_include.php';

$src = strtolower($_GET['src'] ?? $argv[1] ?? '');
if ($src !== 'eur' && $src !== 'usd' && $src !== 'jpy' && $src !== 'gbp') {
    echo 'Invalid source.';
    exit(1);
}

// API-Quelle
// Docs: https://docs.bitfinex.com/reference#rest-public-trades
define('API_URL', 'https://api-pub.bitfinex.com/v2/trades/tBTC' . strtoupper($src) . '/hist');

// CSV-Ziel
define('CSV_BASE', DATA_DIR . "bitfinex/btc${src}/bitfinex-tick-btc${src}-");

// Ab welchem Datum soll das Crawling beginnen, wenn noch keine Daten vorliegen?
define('INITIAL_START_DATE', '2011-01-01');

// Status-File: Enthält zuletzt abgefragtes Datum für lückenlose Abfrage
define('STATE_FILE', DATA_DIR . "bitfinex/btc${src}/.state");

if (!file_exists(STATE_FILE)) {
    
    // Noch keine Daten gesammelt. Datei erzeugen und von vorne beginnen.
    echo 'Starting new.' . PHP_EOL;
    $isFirst = true;
    $startQuery = new \DateTime(INITIAL_START_DATE);
    $lastIDs = [];
    
} else {
    
    // Datensatz existiert, fortsetzen
    $isFirst = false;
    $lastDatasets = file(STATE_FILE);
    
    echo 'Last modified: ' . strftime('%Y-%m-%d %H:%M:%S', filemtime(STATE_FILE)) . PHP_EOL;
    echo 'Reading state from file: ' . STATE_FILE . ':' . PHP_EOL;
    
    foreach($lastDatasets as $i => $dataset) {
        $lastDatasets[$i] = explode(",", $dataset);
        $lastIDs[] = $lastDatasets[$i][0];
    }
    
    $lastDatasetTime = readISODate(end($lastDatasets)[1]);
    
    echo 'Last dataset: ' . end($lastDatasets)[0] . " @ " . $lastDatasetTime->format('Y-m-d H:i:s.u') . PHP_EOL;

    // Zuletzt erfasster Datensatz ist weniger als eine Minute alt:
    // Verhindere zu häufige Abfragen
    if ($lastDatasetTime > ( (new DateTime())->sub(new \DateInterval('PT1M')))) {
        echo 'Last dataset is too recent. Stop.';
        exit(1);
    }
    
    unset($lastDatasets);

    // Eine Sekunde in die Vergangenheit abfragen, um ggf. doppelte Aufträge in der
    // selben Sekunde vollständig zu erfassen
    $startQuery = clone $lastDatasetTime;
    $startQuery->sub(new DateInterval('PT1S'));
}


// API abfragen
echo 'Reading data for BTC/' . strtoupper($src) . PHP_EOL;
echo PHP_EOL . 'Querying from: ' . $startQuery->format('Y-m-d H:i:s.u') . PHP_EOL;

$url = API_URL . '?' . http_build_query([
    'start' => getUnixTimeWithMilliseconds($startQuery),
    'limit' => 10000,
    'sort' => 1,
]);

echo 'Querying ' . $url . PHP_EOL;

$json = file_get_contents($url);

// floats als string beibehalten, da die Werte für PHP zu groß werden können...
$json = preg_replace('/((?:-)?\d+(?:\.\d+)?(?:e-\d+)?)/', '"$1"', $json);

$data = json_decode($json);
if (!is_array($data)) {
    notifyAboutCriticalError('Could not decode response: ' . json_last_error_msg());
    echo 'Could not decode response: ' . json_last_error_msg() . PHP_EOL;
    echo 'Received data: ' . PHP_EOL;
    infoLog('Decoding data failed.');
    var_dump($json);
    exit(1);
}

echo 'Received data: ' . strlen($json) . ' bytes / '. count($data) . ' datasets' . PHP_EOL . PHP_EOL;

if (empty($data)) {
    notifyAboutCriticalError('Received dataset is empty');
    echo 'Received dataset is empty.';
    exit(1);
}

// Nach Datum (!) aufsteigend sortieren (= zweite Spalte)
// IDs sind nicht zwingend aufsteigend, da ID = Auftrag, Ausführung ggf. später
// Bei gleichem Zeitstempel nach ID sortieren
usort($data, function($a, $b) {
    $diff = $a[1] - $b[1];
    if ($diff === 0) {
        return $a[0] - $b[0];
    }
    return $diff;
});


// Ergebnis zusammenstellen
$hasDupes = false;
$result = [];
$lastResults = [];
$numNewDatasets = 0;
foreach ($data as $tick) {
    
    // Datum einlesen: Unix-Timestamp mit Millisekunden
    $time = DateTime::createFromFormat('U.u', sprintf('%f', $tick[1] / 1000));
    
    // Auf gültigen Zeitraum prüfen
    if (
        $time === false ||
        (int)$time->format('Y') < 2010 ||
        (int)$time->format('Y') > 2030
    ) {
        notifyAboutCriticalError('Parsed date outside valid range (2010-2030)');
        infoLog('Parsed date outside valid range (2010-2030)!');
        echo 'Parsed: ' . getISODate($time) . PHP_EOL;
        var_dump($tick);
        echo 'Result set:' . PHP_EOL;
        var_dump($json);
        exit(1);
    }
    
    $tick[1] = getISODate($time);
    $tickLine = implode(' / ', $tick);
    
    // Dieser Datensatz ist bereits erfasst worden
    if (in_array($tick[0], $lastIDs)) {
        $hasDupes = true;
        echo 'Dupe: ' . $tickLine . PHP_EOL;
        continue;
    }
    
    echo 'Tick: ' . $tickLine . PHP_EOL;
    
    if (!isset($result[$time->format("Y-m")])) {
        $result[$time->format("Y-m")] = "";
    }
    
    // Datensatz ist bereits in richtigem Format:
    // ID, Zeitstempel, Volumen (+/- mit Art), Preis
    $resultLine = implode(',', array_values($tick));
    $result[$time->format("Y-m")] .= $resultLine . PHP_EOL;
    
    // Letzte 10.000 Datensätze in Status speichern, um Duplikate zu vermeiden
    $lastResults[] = $resultLine;
    $numNewDatasets++;
}

// Keine neuen Datensätze: Ende
if (empty($result)) {
    echo 'No new datasets.';
    exit(1);
}

if (!$hasDupes && !$isFirst) {
    notifyAboutCriticalError('No duplicates found');
    echo 'No duplicates found.';
    exit(1);
}

// Ergebnis in Datei speichern
foreach ($result as $month => $ticks) {
    $csvFile = CSV_BASE . $month . ".csv.gz";
    
    // Monatsdatei noch nicht vorhanden
    if (!file_exists($csvFile)) {
        file_put_contents($csvFile, gzencode('ID,Time,Amount,Price' . PHP_EOL));
    }
    
    $ticks = gzencode($ticks);
    file_put_contents($csvFile, $ticks, FILE_APPEND);
}

infoLog(
    'BTC' . strtoupper($src) . ': ' .
    'Collected ' . number_format($numNewDatasets, 0, ',', '.') . ' new datasets.'
);
file_put_contents(STATE_FILE, implode(PHP_EOL, $lastResults));

