<?php

namespace RrdCached;

use Exception as Exception;
use Socket\Raw\Factory;
use Socket\Raw\Socket;

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
     *
     * @param $socketPath
     */
    public function __construct($socketPath = 'unix:///var/run/rrdcached.sock')
    {
        $this->socketPath = $socketPath;
    }

    /**
     * @throws RrdCachedException
     *
     * @return bool
     */
    public function connect()
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
     * @throws RrdCachedException
     *
     * @return bool
     */
    public function disconnect()
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
     *
     * @throws RrdCachedException
     *
     * @return int number of bytes actually written
     */
    public function write($command)
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
     *
     * @return int
     */
    public function parseServerLn($line)
    {
        $space = strpos($line, ' ');
        $this->lastCode = (int) substr($line, 0, $space);
        $this->lastMessage = trim(substr($line, $space + 1));

        return $this->lastCode;
    }

    /**
     * @param string $param
     *
     * @throws RrdCachedException
     *
     * @return bool
     */
    protected function checkLastCode($param = '')
    {
        if (0 <= $this->getLastCode()) {
            return true;
        } else {
            if ('' !== $param) {
                $addMsg = ': '.$param;
            }

            throw new RrdCachedException($this->getLastMessage().$addMsg, $this->getLastCode());
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
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function help($command)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write('HELP '.$command.PHP_EOL);
        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @throws RrdCachedException
     *
     * @return string
     */
    public function stats()
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write('STATS'.PHP_EOL);

        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @return bool
     */
    public function quit()
    {
        $this->write('QUIT'.PHP_EOL);
        $this->disconnect();

        return true;
    }

    /**
     * @param $fileName
     * @param $options
     * @param bool $autoCreate
     *
     * @throws RrdCachedException
     *
     * @return bool|string
     */
    public function update($fileName, $options, $autoCreate = true)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command'  => 'update',
                'fileName' => $fileName,
                'options'  => $options,
            ];
        } else {
            $this->write('UPDATE '.$fileName.' '.implode(':', $options).PHP_EOL);

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
     *
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
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function create($fileName, $options)
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
                'command'  => 'create',
                'fileName' => $fileName,
                'options'  => $workOptions,
            ];
        } else {
            $this->write('CREATE '.$fileName.' '.implode(' ', $workOptions).PHP_EOL);

            if ($this->autoParse) {
                return $this->readAndParse();
            }
        }
    }

    /**
     * @return bool
     */
    public function batchBegin()
    {
        $this->write('BATCH'.PHP_EOL);
        $this->batchMode = true;
        $this->readAndParse();

        return true;
    }

    /**
     * @throws RrdCachedException
     *
     * @return bool
     */
    public function batchCommit()
    {
        $this->batchMode = false;
        $this->autoParse = false;

        foreach ($this->batchCommands as $k => $item) {
            switch ($item['command']) {

                case 'create':
                    $this->create($item['fileName'], $item['options']);
                    break;

                case 'update':
                    $this->update($item['fileName'], $item['options']);
                    break;

                case 'flush':
                    $this->flush($item['fileName']);
                    break;

                case 'forget':
                    $this->forget($item['fileName']);
                    break;

                case 'wrote':
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
                case 'update':
                    $this->updateErrorHandler($batchItem['fileName'], $batchItem['options']);
                    break;

                case 'create':
                    if (0 < strpos($v, 'File exists')) {
                        continue 2;
                    }

                    throw new RrdCachedException('Error create file '.$batchItem['fileName']);
            }
        }

        $this->autoParse = true;

        return true;
    }

    /**
     * @param $fileName
     *
     * @return string
     */
    public function flush($fileName)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command'  => 'flush',
                'fileName' => $fileName,
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
     *
     * @return string
     */
    public function wrote($fileName)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command'  => 'wrote',
                'fileName' => $fileName,
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
     * @throws RrdCachedException
     *
     * @return string
     */
    public function flushAll()
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FLUSHALL not allowed in batch mode');
        }

        $this->write('FLUSHALL'.PHP_EOL);

        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @param $fileName
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function pending($fileName)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method PENDING not allowed in batch mode');
        }

        $this->write("PENDING $fileName".PHP_EOL);

        $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return true;
        }
    }

    /**
     * @param $fileName
     *
     * @return string
     */
    public function forget($fileName)
    {
        if ($this->batchMode) {
            $this->batchCommands[] = [
                'command'  => 'forget',
                'fileName' => $fileName,
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
     * @throws RrdCachedException
     *
     * @return string
     */
    public function queue()
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method QUEUE not allowed in batch mode');
        }

        $this->write('QUEUE'.PHP_EOL);

        $result = $this->readAndParse();
        if ($this->checkLastCode()) {
            return $result;
        }
    }

    /**
     * @param $fileName
     * @param $options
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function fetch($fileName, $options)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write("FETCH $fileName ".implode(' ', $options)."\n");

        return $this->readAndParse();
    }

    /**
     * @param $fileName
     * @param $options
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function fetchBin($fileName, $options)
    {
        if ($this->batchMode) {
            throw new RrdCachedException('Method FETCH not allowed in batch mode');
        }

        $this->write("FETCHBIN $fileName ".implode(' ', $options)."\n");

        $result = $this->readAndParse();
        if ($this->checkLastCode($fileName)) {
            return $result;
        }
    }

    /**
     * @param $fileName
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function info($fileName)
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
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function first($fileName, $raaIndex = 0)
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
     *
     * @throws RrdCachedException
     *
     * @return string
     */
    public function last($fileName)
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
    public function isConnected()
    {
        return $this->connected;
    }

    /**
     * @return bool|string
     */
    public function getLastMessage()
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
    public function getLastCode()
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
    public function getSocket()
    {
        if ($this->socket instanceof Socket) {
            return $this->socket;
        } else {
            return false;
        }
    }
}
