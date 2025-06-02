<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Types\Node;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\Feature\NodeReferencing\Dto\SerializedNodeReferences;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;

trait ReferenceRelation
{
    private readonly ClientInterface $client;

    private function clearReferenceRelations(
        NodeAggregateId $sourceNodeAggregateId,
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        \DateTimeImmutable $lastModified,
        \DateTimeImmutable $originalLastModified,
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (sourceNode:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                OPTIONAL MATCH (sourceNode)-[rel:REFERENCE]->()
                DELETE rel
                SET sourceNode.lastModified = $lastModified
                SET sourceNode.originalLastModified = $originalLastModified',
                [
                    'aggregateId' => $sourceNodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    'lastModified' => $lastModified->format(\DateTimeInterface::ATOM),
                    'originalLastModified' => $originalLastModified->format(\DateTimeInterface::ATOM),
                ]
            )
        );
    }
    private function createReferenceRelations(
        NodeAggregateId $sourceNodeAggregateId,
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        SerializedNodeReferences $references,
        \DateTimeImmutable $lastModified,
        \DateTimeImmutable $originalLastModified,
    ): void
    {
        foreach ($references as $reference) {
            $position = 0;
            foreach ($reference->references as $nodeReference) {
                $this->client->runStatement(Statement::create(
                    'MATCH (sourceNode:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    MATCH (targetNode:Node {aggregateId: $referencedNodeAggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    MERGE (sourceNode)-[:REFERENCE {referenceName: $referenceName, position: $position}]->(targetNode)
                    SET sourceNode.lastModified = $lastModified
                    SET sourceNode.originalLastModified = $originalLastModified',
                    [
                        'aggregateId' => $sourceNodeAggregateId->value,
                        'contentStreamId' => $contentStreamId->value,
                        'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                        'referenceName' => $reference->referenceName->value,
                        'position' => $position,
                        'referencedNodeAggregateId' => $nodeReference->targetNodeAggregateId->value,
                        'lastModified' => $lastModified->format(\DateTimeInterface::ATOM),
                        'originalLastModified' => $originalLastModified->format(\DateTimeInterface::ATOM),
                    ]
                ));
                $position++;
            }
        }
    }

    private function copyReferenceRelations(
        Node $sourceNode,
        Node $targetNode,
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                '
                MATCH (targetNode) WHERE ID(targetNode) = $targetNodeId
                MATCH (sourceNode)-[ref:REFERENCE]->(targetNode) WHERE ID(sourceNode) = $sourceNodeId
                CALL apoc.refactor.from(
                    ref,
                    targetNode
                ) YIELD input, output
                RETURN output',
                [
                    'sourceNodeId' => $sourceNode->getId(),
                    'targetNodeId' => $targetNode->getId(),
                ]
            )
        );
    }
}
