<?php

namespace OpenTracing\Predis;

use OpenTracing\GlobalTracer;
use OpenTracing\Span;
use OpenTracing\Tracer;
use Predis\Client as PredisClient;
use Predis\Command\CommandInterface;

use const OpenTracing\Ext\Tags\COMPONENT;
use const OpenTracing\Ext\Tags\DATABASE_TYPE;
use const OpenTracing\Ext\Tags\DATABASE_STATEMENT;
use const OpenTracing\Ext\Tags\SPAN_KIND;
use const OpenTracing\Ext\Tags\SPAN_KIND_RPC_CLIENT;

class Client extends PredisClient
{
    /** @var \OpenTracing\Tracer */
    private $tracer;

    /** @var \OpenTracing\Span */
    private $parentSpan;

    /** @var string */
    private $tracePrefix = 'Redis';

    public function __construct(
        $parameters = null,
        $options = null,
        Tracer $tracer = null,
        Span $span = null
    )
    {
        parent::__construct($parameters, $options);
        $this->tracer = $tracer ?? GlobalTracer::get();
        $this->parentSpan = $span;
    }

    public function executeCommand(CommandInterface $command)
    {
        $span = $this->tracer->startSpan(
            $this->getOperationName($command->getId()), 
            [
                'child_of' => $this->parentSpan,
                'tags' => [
                    COMPONENT => 'predis',
                    DATABASE_TYPE => 'redis',
                    DATABASE_STATEMENT => $command->getId(),
                    SPAN_KIND => SPAN_KIND_RPC_CLIENT,
                ],
            ]
        );

        $response = parent::executeCommand($command);

        $span->finish();

        return $response;
    }

    private function getOperationName(string $operationName): string
    {
        return $this->tracePrefix . '/' . strtolower($operationName);
    }
}

