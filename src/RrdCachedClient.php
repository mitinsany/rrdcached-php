<?php

namespace RrdCached;

use Socket\Raw\Socket;
use Socket\Raw\Factory;
use RrdCached\RrdCachedException;

class RrdCachedClient
{
    /** @var bool */
    protected $batchEnabled = false;

    /** @var array */
    protected $batchCommands = [];

    /** @var string */
    protected $socketPath;

    /** @var Socket */
    protected $socket;

    function __construct($socketPath = 'unix:///var/run/rrdcached.sock')
    {
        $this->socketPath = $socketPath;
    }

    function connect()
    {
        $factory = new Factory();
        $this->socket = $factory->createClient($this->socketPath);

    }

    function disconnect()
    {
        $this->socket->close();
    }

    function write($command)
    {
        $this->socket->write($command);
        return $this->readAndParse();
    }

    protected function readAndParse()
    {
        $readBuffLen = 2048;
        $serverMessage = $this->socket->read($readBuffLen, PHP_NORMAL_READ);
        $parts = explode(' ', $serverMessage);

        $result = '';

        for ($i = 0; $i < $parts[0] - 1; $i++) {
            $result .= $this->socket->read($readBuffLen, PHP_NORMAL_READ);
        }

        return $result;
    }

    function readAll()
    {
        $readBytes = 16;
        $result = '';
        do {
            $buff = $this->socket->read($readBytes);
            $result .= $buff;
            $len = strlen($buff);
        } while ($readBytes === $len);

        return $result;
    }

    function help($param)
    {
        return $this->writeCommand('HELP', $param);
    }

    protected function writeCommand($command, $param = '')
    {
        if ($this->batchEnabled) {
            $this->batchCommands[] = [
                'command' => $command,
                'param' => $param
            ];

            return true;
        } else {
            return $this->write(sprintf('%s %s' . PHP_EOL, $command, $param));
        }

    }

    function stats()
    {
        return $this->writeCommand('STATS');
    }

    function quit()
    {
        return $this->writeCommand('QUIT');
    }

    function update($fileName, $timestamp, $data)
    {
        return $this->writeCommand('UPDATE', $fileName . ' ' . $timestamp . ':' . $data);
    }

    function batchBegin()
    {
        $this->batchEnabled = true;
    }

    function batchCommit()
    {
        $this->batchEnabled = false;

        $this->writeCommand('BATCH');

        foreach ($this->batchCommands as $k => $item) {
            $this->writeCommand($item['command'], $item['param']);
        }

        return $this->writeCommand('.');
    }

}