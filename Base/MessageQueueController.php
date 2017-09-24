<?php
namespace ZJWorkerman\Base;

use ZJPHP\Base\ZJPHP;
use ZJPHP\Base\Controller;
use ZJPHP\Base\Event;
use ZJPHP\Base\Kit\ArrayHelper;
use ZJPHP\Service\NotifyCenter;
use ZJPHP\Facade\Security;
use WorkermanApp\ManagerProxy\Facade\MQConsumer;
use League\CLImate\CLImate;
use Workerman\Worker;
use Workerman\Connection\AsyncTcpConnection;
use Bunny\Channel;
use Bunny\Async\Client;
use Bunny\Message;

class MessageQueueController extends Controller
{
    protected $processor_addr;

    public function run()
    {
        $args = func_get_args();
        if (count($args) === 1 && ($args[0] instanceof CLImate)) {
            $args = $this->getCliArgs();
        }

        $worker = $args[0];
        $queue = $args[1];
        $routing_keys = $args[2];
        $this->processor_addr = $args[3];


        MQConsumer::start($worker, $queue, $routing_keys, [$this, 'processMessage']);
    }

    public function processMessage(Message $message, Channel $channel, Client $client)
    {
        $message_log_id = MQConsumer::logMessage($message);

        if (!$this->messageCheck($message)) {
            $channel->nack($message, false, false);
            MQConsumer::updateMessageLog($message_log_id, -1);
            return;
        }

        $processor_link = new AsyncTcpConnection('tcp://' . $this->processor_addr);

        $processor_link->onConnect = function ($processor_link) use ($message) {
            $processor_link->send(trim($message->content) . "\n");
        };
        $processor_link->onMessage = function ($processor_link, $response) use ($channel, $message, $message_log_id) {
            $data = json_decode(trim($response), true);
            if (!empty($data['success'])) {
                $channel->ack($message);
                MQConsumer::updateMessageLog($message_log_id, 1);
            } else {
                $requeue = !empty($data['requeue']) ? true : false;
                $channel->nack($message, false, $requeue);
                MQConsumer::updateMessageLog($message_log_id, -1);
            }
        };

        $processor_link->onError = function ($processor_link, $code, $msg) use ($channel, $message, $message_log_id) {
            $channel->nack($message, false, true);
            MQConsumer::updateMessageLog($message_log_id, -1);
        };
        $processor_link->connect();
    }

    protected function messageCheck(Message $message)
    {
        // Verify the signature
        $envelope = json_decode($message->content, true);
        $message_content = $envelope['content'];
        $message_signature = $envelope['signature'];
        $signature_alg = $envelope['alg'];

        if ($message_signature !== Security::hash($message_content, $signature_alg)) {
            return false;
        }
        // Check duplicate
        if (MQConsumer::isMsgDuplicate($message_signature)) {
            return false;
        }

        return true;
    }

    protected function getCliArgs($climate)
    {
        // TBD
    }
}
