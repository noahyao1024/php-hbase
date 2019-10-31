<?php

namespace Hbase;

use TColumnValue;
use TGet;
use THBaseServiceClient;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use Throwable;
use TIOError;
use TPut;
use TResult;
use TScan;

/**
 * Class Hbase
 *
 * @package Hbase
 */
class Hbase
{
    protected static $instance;

    protected static $thriftHost;

    protected static $thriftPort;

    protected $client;

    protected $transport;

    protected $socket;

    /**
     * Method  getInstance
     *
     * @author Morysky
     * @static
     */
    public static function init(string $host, int $port)
    {
        static::$thriftHost = $host;
        static::$thriftPort = $port;
    }

    /**
     * Method  getInstance
     *
     * @return static
     * @author Morysky
     * @static
     */
    public static function getInstance()
    {
        if (static::$instance instanceof static) {
            return static::$instance;
        }

        return static::$instance = new static;
    }

    public function __construct()
    {
        $this->socket    = new TSocket(self::$thriftHost, self::$thriftPort);
        $this->transport = new TBufferedTransport($this->socket);
        $protocol        = new TBinaryProtocol($this->transport);
        $this->client    = new THBaseServiceClient($protocol);

        $this->transport->open();
    }

    public function __destruct()
    {
        $this->transport->close();
    }

    /**
     * @param string $table
     * @param string $rowKey
     *
     * @return array
     * @throws TIOError
     */
    public function get(string $table, string $rowKey)
    {
        $tGet      = new TGet();
        $tGet->row = $rowKey;

        $result = $this->client->get($table, $tGet);

        return static::toArray($result);
    }

    /**
     * @param string $table
     * @param array  $rowKeys
     *
     * @return array
     * @throws TIOError
     */
    public function getMultiple(string $table, array $rowKeys)
    {
        $tGets = [];

        foreach ($rowKeys as $rowKey) {
            $tGet      = new TGet();
            $tGet->row = $rowKey;

            $tGets[] = $tGet;
        }

        $results = $this->client->getMultiple($table, $tGets);

        // Transfer it to plain array
        return array_map(function ($result) {
            return static::toArray($result);
        }, $results);
    }

    /**
     * @param string $table
     * @param string $rowKey
     * @param array  $columns
     *
     * @throws TIOError
     */
    public function put(string $table, string $rowKey, array $columns)
    {
        $tPut      = new tPut();
        $tPut->row = $rowKey;

        foreach ($columns as $column) {
            $tcolumnValue = new TColumnValue();

            $tcolumnValue->family    = $column['family'] ?? '';
            $tcolumnValue->qualifier = $column['qualifier'] ?? '';
            $tcolumnValue->value     = $column['value'] ?? '';

            $tPut->columnValues[] = $tcolumnValue;
        }

        $this->client->put($table, $tPut);
    }

    public function setRecvTimeout(int $millisecond)
    {
        $this->socket->setRecvTimeout($millisecond);
    }

    public function setSendTimeout(int $millisecond)
    {
        $this->socket->setSendTimeout($millisecond);
    }

    public function openScanner($table, array $scanParamters)
    {
        $scan = new TScan($scanParamters);

        return $this->client->openScanner($table, $scan);
    }

    public function scannerRowRange($scannerId, $numRows, $retryTimes)
    {
        while (true) {
            try {
                $rows = $this->client->getScannerRows($scannerId, $numRows);
                if (empty($rows)) {
                    break;
                }
                foreach ($rows as $row) {
                    yield $row;
                }
            } catch (Throwable $exception) {
                if (strpos($exception->getMessage(), 'TSocket: timed out') !== false) {
                    if (--$retryTimes < 0) {
                        throw $exception;
                        break;
                    }
                }
            }
        }
    }

    public function closeScanner($scannerId)
    {
        $this->client->closeScanner($scannerId);
    }

    private static function toArray($tResult)
    {
        if (!$tResult instanceof TResult) {
            return [];
        }

        return array_map(function ($column) {
            return [
                'family'    => $column->family,
                'qualifier' => $column->qualifier,
                'value'     => $column->value,
                'timestamp' => $column->timestamp,
            ];
        }, $tResult->columnValues);
    }
}
