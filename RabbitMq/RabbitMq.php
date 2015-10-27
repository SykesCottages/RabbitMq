<?php

namespace Sykes\RabbitMq;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use AMQPQueue;

/**
 * Wrapper class to manage requests to RabbitMq
 *
 * @class RabbitMq
 */
class RabbitMq
{
    /**
     * @type AMQPConnection
     */
    private $connection;

    /**
     * @type AMQPExchange
     */
    private $exchange;

    /**
     * @type AMQPChannel
     */
    private $channel;

    /**
     * @var AMQPQueue
     */
    private $queue;

    private $host = '127.0.0.1';
    private $port = 5672;
    private $username = 'guest';
    private $password = 'guest';
    private $vhost = '/';

    /**
     * Connect to RabbitMq based on environment
     */
    public function connect()
    {
        $this->connection = new AMQPConnection(
            [
                'host'     => $this->host,
                'vhost'    => $this->vhost,
                'port'     => $this->port,
                'login'    => $this->username,
                'password' => $this->password
            ]
        );
        $this->connection->connect();
        $this->channel  = new AMQPChannel($this->connection);
        $this->exchange = new AMQPExchange($this->channel);
    }

    /**
     * Close the connection
     */
    public function disconnect()
    {
        $this->connection->disconnect();
    }

    /**
     * Adds a message into a queue
     *
     * @param string $queue   Queue Name, cannot start with `amq.`
     * @param string $message Message to send
     */
    public function add_to_queue($queue, $message)
    {
        $this->connect();
        $this->exchange->publish(
            $message,
            $queue
        );
        $this->disconnect();
    }

    /**
     * Consume the queue and bind this to a callback, the callback should be wrapped in a try catch,
     * if an exception has been thrown then the message should be rejected into the dead letter
     * queue
     *
     * @param string                    $queue    Name of the queue, cannot start with `amq.`
     * @param string | array | callable $callback Callback function, either the name of the function if
     *                                            its in global scope, an array if its in object scope
     *                                            i.e. [ $this->feed_model, 'test' ], or an anonymous
     *                                            function
     */
    public function consume($queue, $callback)
    {
        $this->connect();
        $this->queue = new AMQPQueue($this->channel);
        $this->queue->setName($queue);
        $this->queue->consume($callback);

        $this->disconnect();
    }

    /**
     * Acknowledge a message
     *
     * @param $deliveryTag
     */
    public function acknowledge($deliveryTag)
    {
        $this->queue->ack($deliveryTag);
    }

    /**
     * Reject a message
     *
     * @param $deliveryTag
     */
    public function negativeAcknowledge($deliveryTag)
    {
        $this->queue->nack($deliveryTag);
    }

    /**
     * Purge a queue, used in unit tests
     *
     * @param $queue
     */
    public function purge($queue)
    {
        $this->connect();
        $this->queue = new AMQPQueue($this->channel);
        $this->queue->setName($queue);
        $this->queue->purge();
        $this->disconnect();
    }

    /**
     * @param string $host
     */
    public function setHost($host)
    {
        $this->host = $host;
    }

    /**
     * @param string $password
     */
    public function setPassword($password)
    {
        $this->password = $password;
    }

    /**
     * @param int $port
     */
    public function setPort($port)
    {
        $this->port = $port;
    }

    /**
     * @param string $username
     */
    public function setUsername($username)
    {
        $this->username = $username;
    }

    /**
     * @param string $vhost
     */
    public function setVhost($vhost)
    {
        $this->vhost = $vhost;
    }
}
