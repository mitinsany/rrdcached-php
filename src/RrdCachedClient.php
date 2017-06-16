<?php

namespace RrdCached;

use Socket\Raw\Socket;
use Socket\Raw\Factory;
use \Exception as Exception;

class RrdCachedClient
{
    /** @var bool */
    protected $autoParse = true;

    /** @var bool */
    protected $batchMode = false;

    /** @var */
    protected $batchCommands = [];

    /** @var */
    protected $socketPath;

    /** @var Socket */
    protected $socket;

    /** @var */
    public $defaultCreateParams = [];

    /** @var int */
    public $defaultCreateStep = 300;

    /** @var bool */
    protected $connected = false;

    /** @var string */
    protected $lastMessage = '';

    /** @var int */
    protected $lastCode = null;

    /**
     * RrdCachedClient constructor.
     * @param $socketPath
     */
    function __construct($socketPath = 'unix:///var/run/rrdcached.sock')
    {
        $this->socketPath = $socketPath;
    }

    /**
     * @return bool
     * @throws RrdCachedException
     */
    function connect()
    {
        $factory = new Factory();

        try {
            $this->socket = $factory->createClient($this->socketPath);
        } catch (Exception $e) {
            throw new RrdCachedException($e->getMessage(), $e->getCode());
        }

        $this->connected = true;
        return true;
    }

    /**
     * @return bool
     * @throws RrdCachedException
     */
    function disconnect()
    {
        try {
            $this->socket->close();
            $this->socket = null;
        } catch (Exception $e) {
            throw new RrdCachedException($e->getMessage(), $e->getCode());
        }

        $this->connected = false;
        return true;
    }

    /**
     * @param $command
     * @return int number of bytes actually written
     * @throws RrdCachedException
     */
    function write($command)
    {
        try {
            $written = $this->socket->write($command);
        } catch (Exception $e) {
            throw new RrdCachedException($e->getMessage(), $e->getCode());
        }

        return $written;
    }

    /**
     * @param $line
     * @return int
     */
    public function parseServerLn($line)
    {
        $space = strpos($line, ' ');
        $this->lastCode = (int)substr($line, 0, $space);
        $this->lastMessage = trim(substr($line, $space + 1));

        return $this->lastCode;
    }

    /**
     * @param string $param
     * @return bool
     * @throws RrdCachedException
     */
    protected function checkLastCode($param = '')
    {
        if (0 <= $this->getLastCode()) {
            return true;
        } else {
            if ('' !== $param) {
                $addMsg = ': ' . $param;
            }
            throw new RrdCachedException($this->getLastMessage() . $addMsg, $this->getLastCode());
        }
    }

    /**
     * @return string
     */
    protected function readAndParse()
    {
        $readBuffLen = 2048;
        $serverMessage = $this->socket->read($readBuffLen, PHP_NORMAL_READ);
        $this->parseServerLn($serverMessage);

        $result = '';

        for ($i = 1; $i <= $this->lastCode; $i++) {
            $result .= $this->socket->read($readBuffLen, PHP_NORMAL_READ);
        }

        return '' !== $result ? $result : $serverMessage;
    }

    /**
     * @param $command
     * @return string
     * @throws RrdCachedException
     */
    function help($command)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write('HELP ' . $command . PHP_EOL);
        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @return string
     * @throws RrdCachedException
     */
    function stats()
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write('STATS' . PHP_EOL);

        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @return bool
     */
    function quit()
    {
        $this->write('QUIT' . PHP_EOL);
        $this->disconnect();
        return true;
    }

    /**
     * @param $fileName
     * @param $options
     * @param bool $autoCreate
     * @return bool|string
     * @throws RrdCachedException
     */
    function update($fileName, $options, $autoCreate = true)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command' => 'update',
                'fileName' => $fileName,
                'options' => $options
            ];
        } else {
            $this->write('UPDATE ' . $fileName . ' ' . implode(':', $options) . PHP_EOL);

            if ($this->autoParse) {
                $this->readAndParse();
                if (true === $autoCreate) {
                    if (-1 === $this->getLastCode()) {
                        return $this->updateErrorHandler($fileName, $options);
                    }
                } else {
                    throw new RrdCachedException($this->lastMessage, $this->lastCode);
                }
            }
        }
        return true;
    }


    /**
     * @param $fileName
     * @param $options
     * @return string
     */
    protected function updateErrorHandler($fileName, $options)
    {
        //$this->autoParse = false;

        $this->create($fileName, $this->defaultCreateParams);
        $result = $this->update($fileName, $options);

        //$this->autoParse = true;

        return $result;
    }

    /**
     * @param $fileName
     * @param $options
     * @return string
     * @throws RrdCachedException
     */
    function create($fileName, $options)
    {
        if (0 < count($options)) {
            $workOptions = $options;
        } elseif (0 < count($this->defaultCreateParams)) {
            $workOptions = $this->defaultCreateParams;
        } else {
            throw new RrdCachedException('Missing options for create RRD file. Set of options to parameter
            defaultCreateParams or send as parameter in create function');
        }


        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command' => 'create',
                'fileName' => $fileName,
                'options' => $workOptions
            ];
        } else {
            $this->write('CREATE ' . $fileName . ' ' . implode(' ', $workOptions) . PHP_EOL);

            if ($this->autoParse) {
                return $this->readAndParse();
            }
        }
    }

    /**
     * @return bool
     */
    function batchBegin()
    {
        $this->write('BATCH' . PHP_EOL);
        $this->batchMode = true;
        $this->readAndParse();

        return true;
    }

    /**
     * @return bool
     * @throws RrdCachedException
     */
    function batchCommit()
    {
        $this->batchMode = false;
        $this->autoParse = false;

        foreach ($this->batchCommands as $k => $item) {

            switch ($item['command']) {

                case 'create':
                    $this->create($item['fileName'], $item['options']);
                    break;

                case 'update';
                    $this->update($item['fileName'], $item['options']);
                    break;

                case 'flush';
                    $this->flush($item['fileName']);
                    break;

                case 'forget';
                    $this->forget($item['fileName']);
                    break;

                case 'wrote';
                    $this->wrote($item['fileName']);
                    break;
            }
        }

        $this->write(".\n");
        $batchResultRaw = $this->readAndParse();
        $batchResult = explode("\n", rtrim($batchResultRaw));

        if (1 === count($batchResult) && 0 === $this->parseServerLn($batchResult[0])) {
            return true;
        }

        foreach ($batchResult as $k => $v) {

            $returnCommandNum = $this->parseServerLn($v) - 1;
            $batchItem = $this->batchCommands[$returnCommandNum];

            switch ($batchItem['command']) {
                case 'update';
                    $this->updateErrorHandler($batchItem['fileName'], $batchItem['options']);
                    break;

                case 'create':
                    if (0 < strpos($v, 'File exists')) {
                        continue;
                    }
                    throw new RrdCachedException('Error create file ' . $batchItem['fileName']);

            }
        }

        $this->autoParse = true;

        return true;
    }

    /**
     * @param $fileName
     * @return string
     */
    function flush($fileName)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command' => 'flush',
                'fileName' => $fileName
            ];
        } else {
            $this->write("flush $fileName\n");

            if ($this->autoParse) {
                $result = $this->readAndParse();
                if ($this->checkLastCode()) {
                    return $result;
                }
            }
        }
    }

    /**
     * @param $fileName
     * @return string
     */
    function wrote($fileName)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command' => 'wrote',
                'fileName' => $fileName
            ];
        } else {
            $this->write("wrote $fileName\n");

            if ($this->autoParse) {
                $result = $this->readAndParse();
                if ($this->checkLastCode()) {
                    return $result;
                }
            }
        }
    }

    /**
     * @return string
     * @throws RrdCachedException
     */
    function flushAll()
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FLUSHALL not allowed in batch mode');
        }

        $this->write('FLUSHALL' . PHP_EOL);

        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @param $fileName
     * @return string
     * @throws RrdCachedException
     */
    function pending($fileName)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method PENDING not allowed in batch mode');
        }

        $this->write("PENDING $fileName" . PHP_EOL);

        $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return true;
        }
    }

    /**
     * @param $fileName
     * @return string
     */
    function forget($fileName)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command' => 'forget',
                'fileName' => $fileName
            ];
        } else {
            $this->write("FORGET $fileName\n");

            if ($this->autoParse) {
                $this->readAndParse();
                if ($this->checkLastCode($fileName)) {
                    return true;
                }
            }
        }
    }

    /**
     * @return string
     * @throws RrdCachedException
     */
    function queue()
    {

        if ($this->batchMode) {
            throw new RrdCachedException('Method QUEUE not allowed in batch mode');
        }

        $this->write('QUEUE' . PHP_EOL);

        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @param $fileName
     * @param $options
     * @return string
     * @throws RrdCachedException
     */
    function fetch($fileName, $options)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write("FETCH $fileName " . implode(' ', $options) . "\n");
        return $this->readAndParse();
    }

    /**
     * @param $fileName
     * @param $options
     * @return string
     * @throws RrdCachedException
     */
    function fetchBin($fileName, $options)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write("FETCHBIN $fileName " . implode(' ', $options) . "\n");

        $result = $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return $result;
        }
    }

    /**
     * @param $fileName
     * @return string
     * @throws RrdCachedException
     */
    function info($fileName)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method INFO not allowed in batch mode');
        }

        $this->write("INFO $fileName\n");

        $result = $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return $result;
        }
    }

    /**
     * @param $fileName
     * @param int $raaIndex
     * @return string
     * @throws RrdCachedException
     */
    function first($fileName, $raaIndex = 0)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FIRST not allowed in batch mode');
        }

        $this->write("FIRST $fileName $raaIndex\n");

        $result = $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return $result;
        }
    }

    /**
     * @param $fileName
     * @return string
     * @throws RrdCachedException
     */
    function last($fileName)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method LAST not allowed in batch mode');
        }

        $this->write("LAST $fileName\n");

        $result = $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return $result;
        }
    }

    /**
     * @return bool
     */
    function isConnected()
    {
        return $this->connected;
    }

    /**
     * @return bool|string
     */
    function getLastMessage()
    {
        if (null !== trim($this->lastMessage)) {
            return $this->lastMessage;
        } else {
            return false;
        }
    }

    /**
     * @return bool|int
     */
    function getLastCode()
    {
        if (null !== $this->lastCode) {
            return $this->lastCode;
        } else {
            return false;
        }
    }

    /**
     * @return bool|Socket
     */
    function getSocket()
    {
        if ($this->socket instanceof Socket) {
            return $this->socket;
        } else {
            return false;
        }
    }
}