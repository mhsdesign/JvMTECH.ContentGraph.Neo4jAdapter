<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Types\Node;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\Feature\NodeReferencing\Dto\SerializedNodeReferences;

use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Node\ReferenceName;
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
        SerializedNodeReferences $newReferences,
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (sourceNode:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                OPTIONAL MATCH (sourceNode)-[rel:REFERENCE]->()
                WHERE rel.referenceName IN $referenceNames
                DELETE rel
                SET sourceNode.lastModified = $lastModified
                SET sourceNode.originalLastModified = $originalLastModified',
                [
                    'aggregateId' => $sourceNodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    'referenceNames' => array_map(fn(ReferenceName $referenceName) => $referenceName->value, $newReferences->getReferenceNames()),
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
                $result = $this->client->runStatement(Statement::create(
                    'MATCH (sourceNode:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    MATCH (targetNode:Node {aggregateId: $referencedNodeAggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    MERGE (sourceNode)-[newRef:REFERENCE {referenceName: $referenceName, position: $position}]->(targetNode)
                    SET sourceNode.lastModified = $lastModified
                    SET sourceNode.originalLastModified = $originalLastModified
                    RETURN newRef',
                    [
                        'aggregateId' => $sourceNodeAggregateId->value,
                        'contentStreamId' => $contentStreamId->value,
                        'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                        'referenceName' => $reference->referenceName->value,
                        'position' => $position,
                        'referencedNodeAggregateId' => $nodeReference->targetNodeAggregateId->value,
                        'lastModified' => $lastModified->format(\DateTimeInterface::ATOM),
                        'originalLastModified' => $originalLastModified->format(\DateTimeInterface::ATOM),
                    ],
                ));
                if (empty($result) || !$result->hasKey(0) || !$result->getAsCypherMap(0)->hasKey('newRef')) {
                    continue;
                }
                $referenceResult = $result->getAsCypherMap(0)->getAsRelationship('newRef');;
                if ($nodeReference->properties->count() > 0) {
                    $this->client->runStatement(
                        Statement::create(
                            'MATCH ()-[rel]->() WHERE ID(rel) = $relId
                            SET rel.properties = $properties',
                            [
                                'properties' => json_encode($nodeReference->properties),
                                'relId' => $referenceResult->getId()
                            ]
                        )
                    );
                }
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
