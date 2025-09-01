<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\NodeQueryBuilder;
use JvMTECH\ContentGraph\Neo4jAdapter\Neo4jContentGraphProjection;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;

class Neo4jProjectionContentGraph
{
    public function __construct(
        private readonly ClientInterface $client,
        private bool $debug = false,
    ) {
    }

    public function determineHierarchyRelationPosition(
        ?NodeAggregateId $parentAggregateId,
        ?NodeAggregateId $succeedingSiblingAggregateId,
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        ?NodeAggregateId $childAggregateId = null,
    ): int {
        if (!$parentAggregateId && !$childAggregateId) {
            throw new \InvalidArgumentException(
                'You must specify either parent or child node anchor to determine a hierarchy relation position',
                1519847447
            );
        }
        if ($succeedingSiblingAggregateId) {
            $succeedingNode = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForSubgraph(
                        $contentStreamId,
                        $dimensionSpacePoint,
                        $succeedingSiblingAggregateId,
                    )
                    ->returns('rel,p')
                ->build()
            );
            if ($succeedingNode->hasKey(0) && $succeedingNode->getAsCypherMap(0)->hasKey('rel')) {
                $node = $succeedingNode->getAsCypherMap(0)->getAsRelationship('rel');
                /** @var int $succeedingSiblingNodePosition */
                $succeedingSiblingNodePosition = $node->getProperty('position');
                $parentNode = $succeedingNode->getAsCypherMap(0)->getAsNode('p');
                $parentNodeAggregateId = NodeAggregateId::fromString($parentNode->getProperty('aggregateId'));
                $precedingSiblingNode = $this->client->runStatement(
                    NodeQueryBuilder::createForNodes()
                    ->matchChildrenForSubgraph(
                        $contentStreamId,
                        $dimensionSpacePoint,
                        $parentNodeAggregateId,
                    )
                    ->where('rel.position < $succeedingSiblingNodePosition')
                    ->withParameter('succeedingSiblingNodePosition', $succeedingSiblingNodePosition)
                    ->returns('rel')
                    ->limit(1)
                    ->build()
                );
                if ($precedingSiblingNode->hasKey(0) && $precedingSiblingNode->getAsCypherMap(0)->hasKey('rel')) {
                    $preceedingSiblingNodePosition = $precedingSiblingNode->getAsCypherMap(0)->getAsRelationship('rel')->getProperty('position');
                    return ($succeedingSiblingNodePosition + $preceedingSiblingNodePosition) / 2;
                } else {
                    return $succeedingSiblingNodePosition - Neo4jContentGraphProjection::RELATION_DEFAULT_OFFSET;
                }
            } else {
                //\Neos\Flow\var_dump($succeedingSiblingAggregateId, 'Succeeding sibling not found');
            }
        }
        if ($parentAggregateId) {
            // We need the highest current position for children of the parent-node
            $childNodeRelationResult = $this->client->runStatement(
                $statement = NodeQueryBuilder::createForNodes()
                    ->matchChildrenForSubgraph(
                        $contentStreamId,
                        $dimensionSpacePoint,
                        $parentAggregateId,
                    )
                    ->returns('rel')
                    ->orderBy('rel.position', 'DESC')
                    ->limit(1)
                    ->build()
            );
            if ($childNodeRelationResult->hasKey(0) && $childNodeRelationResult->getAsCypherMap(0)->hasKey('rel')) {
                return $childNodeRelationResult->getAsCypherMap(0)->getAsRelationship('rel')->getProperty('position') + Neo4jContentGraphProjection::RELATION_DEFAULT_OFFSET;
            } else {
                // WHAT TO DO HERE?
            }
        }

        $position = Neo4jContentGraphProjection::RELATION_DEFAULT_OFFSET;
        return $position;
    }

    public function determineRootNodePosition(
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
    ): int
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (r:Root)<-[rel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]-()
                 ORDER BY rel.position
                 LIMIT 1
                 RETURN rel.position as position',
                [
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                ]
            )
        );

        if ($result->isEmpty() || !$result->get(0) || !$result->getAsCypherMap(0)->hasKey('position')) {
            return Neo4jContentGraphProjection::RELATION_DEFAULT_OFFSET;
        }

        return $result->getAsCypherMap(0)->getAsInt('position') + Neo4jContentGraphProjection::RELATION_DEFAULT_OFFSET;
    }
}
