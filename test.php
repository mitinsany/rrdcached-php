<?php

include_once 'vendor/autoload.php';

$r = new \RrdCached\RrdCachedClient();
$r->connect();

$r->batchBegin();

$r->update('x.rrd', 1223661439, '1:2:3');

$r->batchCommit();
