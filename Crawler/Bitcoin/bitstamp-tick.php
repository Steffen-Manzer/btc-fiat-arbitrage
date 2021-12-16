<?php

// Geschrieben für PHP 8.1 (CLI oder HTTP)

require_once '_include.php';

$src = strtolower($_GET['src'] ?? $argv[1] ?? '');
if ($src !== 'eur' && $src !== 'usd' && $src !== 'gbp') {
    echo 'Invalid source.';
    exit(1);
}

// API-Quelle
define('API_URL', 'https://www.bitstamp.net/api/v2/transactions/btc' . $src . '/?time=day');

// CSV-Ziel
define('CSV_BASE', DATA_DIR . "bitstamp/btc${src}/bitstamp-tick-btc${src}-");

// Status-File: Enthält zuletzt abgefragtes Datum für lückenlose Abfrage
define('STATE_FILE', DATA_DIR . "bitstamp/btc${src}/.state");


if (!file_exists(STATE_FILE)) {
    
    // Noch keine Daten gesammelt. Datei erzeugen und neu beginnen.
    // Bitstamp erlaubt keine Abfrage historischer Daten -> Beginn ab heute
    echo 'Starting new.' . PHP_EOL;
    
    // Letzte ID = 0 = Anfang
    $lastDataset = [0];
    $isNew = true;
    
} else {
    
    $isNew = false;
    
    // Datensatz existiert, fortsetzen
    $lastDataset = explode(",", file_get_contents(STATE_FILE));
    
    echo 'Last modified: ' . strftime('%Y-%m-%d %H:%M:%S', filemtime(STATE_FILE)) . PHP_EOL;
    echo 'Reading state from file: ' . STATE_FILE . ':' . PHP_EOL;
    echo 'Last dataset: ID ' . $lastDataset[0] . ' @ ' . $lastDataset[1] . PHP_EOL;
    
    $lastDatasetTime = \DateTime::createFromFormat(TIMEFORMAT_SECONDS, $lastDataset[1]);
    
    // Letzter Datensatz zuletzt vor weniger als einer Stunde eingelesen
    if ($lastDatasetTime > ( (new \DateTime())->sub(new \DateInterval('PT1H')))) {
        echo 'Last dataset is too recent. Stop.';
        exit(1);
    }
}


// API abfragen
echo 'Reading data for BTC/' . strtoupper($src) . PHP_EOL;
echo 'Querying ' . API_URL . PHP_EOL;

$json = file_get_contents(API_URL);

$data = json_decode($json);
if (!is_array($data)) {
    notifyAboutCriticalError('Could not decode response: '. json_last_error_msg());
    echo 'Could not decode response: ' . json_last_error_msg() . PHP_EOL;
    echo 'Received data: ' . PHP_EOL;
    infoLog('Decoding data failed.');
    var_dump($json);
    exit(1);
}


echo 'Received data: ' . round(strlen($json)/1000) . ' kB / '. count($data) . ' datasets' . PHP_EOL . PHP_EOL;

if (empty($data)) {
    notifyAboutCriticalError('Received dataset is empty');
    echo 'Received dataset is empty.';
    exit(1);
}

// Neueste zuerst, daher Reihenfolge umkehren
$data = array_reverse($data);

// Ergebnis zusammenstellen
$hasDupes = false;
$result = [];
$numNewDatasets = 0;
$lastDatasetFound = '';
foreach ($data as $tick) {
    
    // Zeit einlesen: Unix-Timestamp
    $time = new \DateTime();
    $time->setTimestamp($tick->date);
    
    // Auf gültigen Zeitraum prüfen
    if (
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
    
    $tick->date = getISODateSeconds($time);
    
    // Datensatz aufbereiten: ID, Zeitstempel, Volumen, Preis, Art
    $tickLine = sprintf(
        '%s,%s,%s,%s,%s',
        $tick->tid,
        $tick->date,
        $tick->amount,
        $tick->price,
        $tick->type
    );
    
    // Dieser Datensatz ist bereits erfasst worden
    if ($tick->tid <= $lastDataset[0]) {
        $hasDupes = true;
        echo 'Dupe: ' . $tickLine . PHP_EOL;
        continue;
    }
    
    if (!isset($result[$time->format("Y-m")])) {
        $result[$time->format("Y-m")] = "";
    }
    
    // Datensatz hinzufügen
    echo 'Tick: ' . $tickLine . PHP_EOL;
    $result[$time->format("Y-m")] .= $tickLine . PHP_EOL;
    $lastDatasetFound = $tickLine;
    $numNewDatasets++;
}

// Keine neuen Datensätze: Ende
if (empty($result)) {
    echo 'No new datasets.';
    exit(1);
}

if (!$hasDupes && !$isNew) {
    notifyAboutCriticalError('No duplicates found');
    echo 'No duplicates found.';
    exit(1);
}

// Ergebnis in Datei speichern
foreach ($result as $month => $ticks) {
    $csvFile = CSV_BASE . $month . ".csv.gz";
    
    // Monatsdatei noch nicht vorhanden
    if (!file_exists($csvFile)) {
        file_put_contents($csvFile, gzencode('ID,Time,Amount,Price,Type' . PHP_EOL));
    }
    
    $ticks = gzencode($ticks);
    file_put_contents($csvFile, $ticks, FILE_APPEND);
}

infoLog(
    'BTC' . strtoupper($src) . ': ' .
    'Collected ' . number_format($numNewDatasets, 0, ',', '.') . ' new datasets.'
);

file_put_contents(STATE_FILE, $lastDatasetFound);


