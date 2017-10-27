<?php
namespace ZJWorkerman\Base;

use ZJPHP\Base\ZJPHP;
use ZJPHP\Base\Controller;
use ZJPHP\Base\Event;
use ZJPHP\Base\Kit\ArrayHelper;
use ZJPHP\Service\NotifyCenter;
use ZJPHP\Facade\Security;
use ZJPHP\Facade\Database;
use ZJWorkerman\Facade\MQConsumer;
use League\CLImate\CLImate;
use Workerman\Worker;
use Workerman\Connection\AsyncTcpConnection;
use Bunny\Channel;
use Bunny\Async\Client;
use Bunny\Message;

class MessageQueueController extends Controller
{
    protected $processorAddr;

    public function run()
    {
        $args = func_get_args();

        $worker = $args[0];
        $queue = $args[1];
        $routing_keys = $args[2];
        $this->processorAddr = $args[3];

        MQConsumer::start($worker, $queue, $routing_keys, [$this, 'processMessage']);
    }

    public function getProcessorLink()
    {
        return new AsyncTcpConnection('tcp://' . $this->processorAddr);
    }

    public function processMessage(Message $message, Channel $channel, Client $client)
    {
        $message_data = MQConsumer::logMessage($message);
        $message_signature = $message_data['signature'];
        $message_log_id = $message_data['id'];

        if (!$this->messageCheck($message_data)) {
            $channel->nack($message, false, false);
            MQConsumer::updateMessageLog($message_log_id, -1);
        } else {
            $processor_link = $this->getProcessorLink();

            $processor_link->onConnect = function ($processor_link) use ($message) {
                $processor_link->send(trim($message->content) . "\n");
            };
            $processor_link->onMessage = function ($processor_link, $response) use ($channel, $message, $message_log_id, $message_signature) {
                $data = json_decode(trim($response), true);
                if (!empty($data['success'])) {
                    $channel->ack($message);
                    MQConsumer::updateMessageLog($message_log_id, 1);
                } else {
                    $requeue = !empty($data['requeue']) && MQConsumer::checkRetry($message_signature);
                    $channel->nack($message, false, $requeue);
                    MQConsumer::updateMessageLog($message_log_id, -1);
                }
                Database::disconnect('all connections');
            };

            $processor_link->onError = function ($processor_link, $code, $msg) use ($channel, $message, $message_log_id, $message_signature) {
                $requeue = MQConsumer::checkRetry($message_signature);
                $channel->nack($message, false, $requeue);
                MQConsumer::updateMessageLog($message_log_id, -1);
                Database::disconnect('all connections');
            };

            $processor_link->connect();
        }

        Database::disconnect('all connections');
    }

    protected function messageCheck($message_data)
    {
        // Verify the signature
        $message_signature = $message_data['signature'];

        if ($message_signature !== Security::hash($message_data['message_content'], $message_data['alg'])) {
            return false;
        }
        // Check duplicate
        if (MQConsumer::isMsgDuplicate($message_signature)) {
            return false;
        }

        return true;
    }
}
