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

    /**
     * Connect to RabbitMq based on environment
     *
     * @param string $host
     * @param int    $port
     * @param string $vhost
     * @param string $login
     * @param string $password
     */
    public function connect($host = '127.0.0.1', $port = 5672, $vhost = '/', $login = 'guest', $password = 'guest')
    {
        $this->connection = new AMQPConnection(
            [
                'host'     => $host,
                'vhost'    => $vhost,
                'port'     => $port,
                'login'    => $login,
                'password' => $password
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
}
