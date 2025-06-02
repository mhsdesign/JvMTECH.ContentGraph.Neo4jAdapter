<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePointSet;
use Neos\ContentRepository\Core\Feature\SubtreeTagging\Dto\SubtreeTag;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;

trait Subtree
{
    private readonly ClientInterface $client;

    private function addSubtreeTag(ContentStreamId $contentStreamId, NodeAggregateId $nodeAggregateId, DimensionSpacePointSet $affectedDimensionSpacePoints, SubtreeTag $tag): void
    {

        $affectedDimensionSpacePointHashes = $affectedDimensionSpacePoints->getPointHashes();
        /** @var SummarizedResult $currentRelationships */
        $currentRelationships = $this->client->runStatement(
            Statement::create(
                'MATCH (:Node {aggregateId: $aggregateId})-[rel:IS_CHILD|IS_ROOT {contentStreamId: $contentStreamId}]->()
                RETURN rel
                ',
                [
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                ]
            )
        );
        /** @var CypherMap $map */
        foreach ($currentRelationships as $map) {
            $relationship = $map->getAsRelationship('rel');
            $relationshipDimensionSpacePointHash = $relationship->getProperty('dimensionSpacePointHash');
            $currentSubtreeTags = [];
            if ($relationship->getProperties()->hasKey('subtreeTags')) {
                $currentSubtreeTags = $relationship->getProperty('subtreeTags')->toArray();
            }
            $relationshipId = $relationship->getId();
            if (in_array($relationshipDimensionSpacePointHash, $affectedDimensionSpacePointHashes)) {
                $currentSubtreeTags[] = $tag->value;
                $this->client->runStatement(
                    Statement::create('MATCH ()-[rel:IS_CHILD|IS_ROOT]->()
                        WHERE id(rel) = $relationshipId
                        SET rel.subtreeTags = $subtreeTags',
                        [
                            'relationshipId' => $relationshipId,
                            'subtreeTags' => $currentSubtreeTags,
                        ],
                    )
                );
            }
        }
    }

    private function removeSubtreeTag(ContentStreamId $contentStreamId, NodeAggregateId $nodeAggregateId, DimensionSpacePointSet $affectedDimensionSpacePoints, SubtreeTag $tag): void
    {

        $affectedDimensionSpacePointHashes = $affectedDimensionSpacePoints->getPointHashes();
        /** @var SummarizedResult $currentRelationships */
        $currentRelationships = $this->client->runStatement(
            Statement::create(
                'MATCH (:Node {aggregateId: $aggregateId})-[rel:IS_CHILD|IS_ROOT {contentStreamId: $contentStreamId}]->()
                RETURN rel
                ',
                [
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                ]
            )
        );
        /** @var CypherMap $map */
        foreach ($currentRelationships as $map) {
            $relationship = $map->getAsRelationship('rel');
            $relationshipDimensionSpacePointHash = $relationship->getProperty('dimensionSpacePointHash');
            $currentSubtreeTags = [];
            if ($relationship->getProperties()->hasKey('subtreeTags')) {
                $currentSubtreeTags = $relationship->getProperty('subtreeTags')->toArray();
            }
            $relationshipId = $relationship->getId();
            if (in_array($relationshipDimensionSpacePointHash, $affectedDimensionSpacePointHashes)) {
                $currentSubtreeTags = array_filter($currentSubtreeTags, fn(string $entry) => $entry !== $tag->value);
                $this->client->runStatement(
                    Statement::create('MATCH ()-[rel:IS_CHILD|IS_ROOT]->()
                        WHERE id(rel) = $relationshipId
                        SET rel.subtreeTags = $subtreeTags',
                        [
                            'relationshipId' => $relationshipId,
                            'subtreeTags' => $currentSubtreeTags,
                        ],
                    )
                );
            }
        }
    }
}
