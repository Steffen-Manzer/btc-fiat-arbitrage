#!/usr/bin/env php
<?php

// Geschrieben für PHP 8.1 (CLI) unter macOS 12.1

require 'config.php';

register_shutdown_function(function() { echo PHP_EOL; });
function dieWithError(string $msg) { echo $msg; exit(1); }
error_reporting(E_ALL);

// Hervorhebungen im Terminal
$bold = shell_exec('tput bold');
$red = shell_exec('tput setaf 1');
$normal = shell_exec('tput sgr0');

$context = stream_context_create([
    'http' => [
        'method' => 'GET',
        'header' =>
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*" . "/" . "*;q=0.8\r\n" . 
            "Cache-Content: max-age=0\r\n" . 
            "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15\r\n" . 
            "Cookie: " . $loginCookie . "\r\n"
    ]
]);

echo "${bold}Suche Daten von TrueFX${normal}" . PHP_EOL;
echo 'Zeitraum: ' . $yearFrom . '-' . $yearTo . ', Monate ' . $monthsFrom . '-' . $monthsTo . PHP_EOL;
echo 'Ziel: ' . $outPath . PHP_EOL;
echo 'Filter: ' . $forexFilter . PHP_EOL;

$cmdOut = '#!/bin/sh' . PHP_EOL;
foreach ($forexFilter as $pair) {
    $cmdOut .= 'mkdir -p ' . escapeshellarg($outPath . $pair . "/") . PHP_EOL;
}

$baseHTML = file_get_contents('https://www.truefx.com/truefx-historical-downloads/', false, $context);

if (preg_match_all('~data-category="(\d+)".+current_category_slug.+value="(.+)".+wpfd-categories.+>(.+)</div>~sU', $baseHTML, $allCategoryHTML, PREG_SET_ORDER) === 0) {
	dieWithError("Konnte Basis-HTML nicht parsen:" . PHP_EOL . $baseHTML);
}

function translateCategoryToMonth(string $category) : string
{
	return (string)[
		'January' => 1,
		'February' => 2,
		'March' => 3,
		'April' => 4,
		'May' => 5,
		'June' => 6,
		'July' => 7,
		'August' => 8,
		'September' => 9,
		'October' => 10,
		'November' => 11,
		'December' => 12
	][$category] ?? $category;
}
$years = [];
foreach ($allCategoryHTML as $categoryHTML) {
	if (preg_match_all('~data-idcat="(\d+)".+title="(.+)"~sU', $categoryHTML[3], $allSubCategoriesRaw, PREG_SET_ORDER) === 0) {
		echo "${bold}${red}Kein Set gefunden für Kategorie #" . $categoryHTML[2] . $normal . PHP_EOL;
	}
	
	$allSubCategories = [];
	foreach ($allSubCategoriesRaw as $subCategory) {
		$allSubCategories[translateCategoryToMonth($subCategory[2])] = $subCategory[1];
	}
	
	$years[$categoryHTML[2]] = [
		'year' => $categoryHTML[2],
		'id' => $categoryHTML[1],
		'months' => $allSubCategories
	]; 
}

for ($year = $yearFrom; $year <= $yearTo; $year++) {
    
	if (!isset($years[$year])) {
		echo "${bold}${red}!!! Kann " . $year . " nicht verarbeiten, nicht gefunden!${normal}" . PHP_EOL;
		continue;
	}
	$thisYear = $years[$year];
	
    for ($month = $monthsFrom; $month <= $monthsTo; $month++) {
        
        echo "${bold}Verarbeite $year / $month ...${normal}" . PHP_EOL;
        
		if (!isset($thisYear['months'][$month])) {
			echo "${bold}${red}!!! Kann " . $year . '/' . $month . " nicht verarbeiten, nicht gefunden!${normal}" . PHP_EOL;
			continue;
		}
		
		$thisMonthId = $thisYear['months'][$month];
		
		// 2020 = 66
		// Juni = 72
		// https://www.truefx.com/wp-admin/admin-ajax.php?juwpfisadmin=false&action=wpfd&task=categories.display&view=categories&id=72&top=66
        
        // get link list
        $url = 
            'https://www.truefx.com/wp-admin/admin-ajax.php?juwpfisadmin=false&action=wpfd&task=files.display&view=files&id=' . 
            $thisMonthId . '&rootCat=42&page=undefined';
            
        echo "Lade $url ..." . PHP_EOL;
        
        $downloadList = file_get_contents($url, false, $context);
        if ($downloadList === false) {
            echo "${bold}${red}!!! Konnte $year / $month nicht laden!" . PHP_EOL;
            continue;
        }
        
        $files = json_decode($downloadList);
        
        // find only relevant links
        if (empty($files) || empty($files->files)) {
            echo "${bold}${red}!!! Keine Links für $year / $month gefunden!${normal}" . PHP_EOL;
            continue;
        }
        
        foreach($files->files as $file) {
        	    
	        $set = strtoupper(substr($file->post_name, 0, 6));
	        if (!in_array($set, $forexFilter)) {
	        	//echo 'Überspringe ' . strtoupper($file->post_name) . ' ...' . PHP_EOL;
	        	continue;
	        }
        
            echo "Verarbeite " . strtoupper($file->post_name) . " ..." . PHP_EOL;
            
            $cmdOut .=
                '[ ! -f ' . escapeshellarg($outPath . $set . "/" . str_replace('.ZIP', '.zip', strtoupper(basename($file->linkdownload)))) . ' ] && ' . 
                'curl ' . implode(' ', [
                    '-v',
                    '-o', escapeshellarg($outPath . $set . "/" . str_replace('.ZIP', '.zip', strtoupper(basename($file->linkdownload)))),
                    escapeshellarg($file->linkdownload)
            ]);
            
            $cmdOut .= " && sleep 1" . PHP_EOL;
        }
    }
    
}

$cmdOut .= 'rm -i $0' . PHP_EOL;

file_put_contents(
    'truefx.sh',
    $cmdOut
);

echo "truefx.sh erstellt.";
echo "${bold}sh truefx.sh${normal} ausführen, um neue Daten via cURL zu laden.";
