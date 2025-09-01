<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use Doctrine\Migrations\Version\State;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\NodeQueryBuilder;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Laudis\Neo4j\Types\Node;
use Laudis\Neo4j\Types\Relationship;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePointSet;
use Neos\ContentRepository\Core\Feature\SubtreeTagging\Dto\SubtreeTag;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;

trait Subtree
{
    private readonly ClientInterface $client;

    private function addSubtreeTag(
        ContentStreamId $contentStreamId,
        NodeAggregateId $nodeAggregateId,
        DimensionSpacePointSet $affectedDimensionSpacePoints,
        SubtreeTag $tag
    ): void {
        $affectedDimensionSpacePointHashes = $affectedDimensionSpacePoints->getPointHashes();
        $result = $this->client->runStatement(
            Statement::create('
            MATCH (currentNode:Node {aggregateId: $aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()
            WHERE rel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes

            // Get existing tags for this specific relationship and merge with new tag
            WITH currentNode, rel,
                 CASE WHEN rel.subtreeTags IS NOT NULL
                      THEN apoc.convert.fromJsonMap(rel.subtreeTags)
                      ELSE {} END as existingTags

            // Set the new tag on the current node, preserving existing tags
            SET rel.subtreeTags = apoc.convert.toJson(apoc.map.setKey(existingTags, $tagValue, true))
            RETURN currentNode, count(*)',
                [
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'affectedDimensionSpacePointHashes' => $affectedDimensionSpacePointHashes,
                    'tagValue' => $tag->value,
                ]
            ));
        $nodeAggregateIdString = $result->getAsCypherMap(0)->getAsNode('currentNode')->getProperty('aggregateId');
        $nodeAggregateId = NodeAggregateId::fromString($nodeAggregateIdString);
        $this->updateInheritedSubtreeTag($nodeAggregateId, $contentStreamId, $affectedDimensionSpacePoints, $tag, false);
    }

    private function removeSubtreeTag(
        ContentStreamId $contentStreamId,
        NodeAggregateId $nodeAggregateId,
        DimensionSpacePointSet $affectedDimensionSpacePoints,
        SubtreeTag $tag,
    ): void {
        $affectedDimensionSpacePointHashes = $affectedDimensionSpacePoints->getPointHashes();
        $result = $this->client->runStatement(
            Statement::create('
            MATCH (currentNode:Node {aggregateId: $aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()
            WHERE rel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes

            // Check if current node has inherited tag from parent
            OPTIONAL MATCH (currentNode)-[currentRel:IS_CHILD {contentStreamId: $contentStreamId}]->(parentNode)-[parentRel:IS_CHILD {contentStreamId: $contentStreamId}]->()
            WHERE currentRel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes
            AND parentRel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes

            // Get current subtree tags as map
            WITH currentNode, rel, parentRel,
                 CASE WHEN rel.subtreeTags IS NOT NULL
                      THEN apoc.convert.fromJsonMap(rel.subtreeTags)
                      ELSE {} END as currentTags,
                 CASE WHEN parentRel IS NOT NULL AND parentRel.subtreeTags IS NOT NULL
                      THEN apoc.convert.fromJsonMap(parentRel.subtreeTags)
                      ELSE {} END as parentTags

            // Update current node: remove if parent does not have tag, or set to inherit if parent still has it
            WITH currentNode, rel, parentRel, currentTags, parentTags,
                 CASE WHEN $tagValue IN keys(parentTags) AND parentTags[$tagValue] IN [true, "inherit"]
                      THEN apoc.map.setKey(apoc.map.removeKey(currentTags, $tagValue), $tagValue, "inherit")
                      ELSE apoc.map.removeKey(currentTags, $tagValue) END as updatedCurrentTags

            SET rel.subtreeTags = CASE WHEN size(keys(updatedCurrentTags)) > 0
                                       THEN apoc.convert.toJson(updatedCurrentTags)
                                       ELSE null END
            RETURN currentNode, count(*)',
                [
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'affectedDimensionSpacePointHashes' => $affectedDimensionSpacePointHashes,
                    'tagValue' => $tag->value,
                ]
            ));
        $nodeAggregateIdString = $result->getAsCypherMap(0)->getAsNode('currentNode')->getProperty('aggregateId');
        $nodeAggregateId = NodeAggregateId::fromString($nodeAggregateIdString);
        $this->updateInheritedSubtreeTag($nodeAggregateId, $contentStreamId, $affectedDimensionSpacePoints, $tag, true);
    }


    private function updateInheritedSubtreeTag(
        NodeAggregateId $pathRootAggregateId,
        ContentStreamId $contentStreamId,
        DimensionSpacePointSet $affectedDimensionSpacePointSet,
        SubtreeTag $tag,
        bool $remove
    ): void {
        foreach ($affectedDimensionSpacePointSet->getPointHashes() as $dimensionSpacePointHash) {
            $this->client->runStatement(
                Statement::create('
                    MATCH p = (n:Node {aggregateId: $pathRootAggregateId})<-[rels:IS_CHILD*]-(c)
                    WHERE all(rel in rels WHERE rel.dimensionSpacePointHash = $dimensionSpacePointHash AND rel.contentStreamId = $contentStreamId)
                    UNWIND relationships(p) as relationship
                    WITH
                        DISTINCT relationship,
                        CASE
                            WHEN relationship.subtreeTags IS NOT NULL
                                THEN apoc.convert.fromJsonMap(relationship.subtreeTags)
                                ELSE {}
                        END as currentSubtreeTags
                    WITH relationship,
                        CASE
                            WHEN $tagValue IN keys(currentSubtreeTags) AND currentSubtreeTags[$tagValue] <> true
                                THEN
                                    CASE WHEN $remove
                                        THEN apoc.map.removeKey(currentSubtreeTags, $tagValue)
                                        ELSE apoc.map.setKey(currentSubtreeTags, $tagValue, "inherit")
                                    END
                                ELSE
                                    CASE
                                        WHEN $remove OR currentSubtreeTags[$tagValue] = true
                                            THEN currentSubtreeTags
                                            ELSE apoc.map.setKey(currentSubtreeTags, $tagValue, "inherit")
                                    END
                        END AS updatedSubtreeTags
                    SET relationship.subtreeTags = CASE
                        WHEN size(keys(updatedSubtreeTags)) > 0
                            THEN apoc.convert.toJson(updatedSubtreeTags)
                            ELSE null
                        END
                    RETURN relationship
                ', [
                    'pathRootAggregateId' => $pathRootAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePointHash,
                    'remove' => $remove,
                    'tagValue' => $tag->value,
                ])
            );
        }
    }

    /** Update existing subtreeTags by setting inherited tags */
    private function updateInheritedSubtreeTags(
        ContentStreamId $contentStreamId,
        NodeAggregateId $nodeAggregateId,
        DimensionSpacePointSet $affectedDimensionSpacePoints,
    ): void {
        $affectedDimensionSpacePointHashes = $affectedDimensionSpacePoints->getPointHashes();
        $this->client->runStatement(
            Statement::create('
            MATCH (currentNode:Node {aggregateId: $aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()
            WHERE rel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes

            // Get existing tags for this specific relationship and merge with new tag
            WITH currentNode, rel,
                 CASE WHEN rel.subtreeTags IS NOT NULL
                      THEN apoc.convert.fromJsonMap(rel.subtreeTags)
                      ELSE {} END as existingTags

            // Set the new tag on the current node, preserving existing tags
            SET rel.subtreeTags = apoc.convert.toJson(existingTags)

            // Handle child nodes - find all descendants
            WITH currentNode
            OPTIONAL MATCH (childNode)-[childRels:IS_CHILD*.. {contentStreamId: $contentStreamId}]->(currentNode)
            WHERE all(childRel IN childRels WHERE childRel.contentStreamId = $contentStreamId
            AND childRel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes)

            // Get the direct parent relationship for each child (the one closest to the child)
            WITH currentNode, childNode, childRels,
                 childRels[size(childRels)-1] as directChildRel

            // Get existing child tags and add inherited tag, preserving existing tags
            WITH currentNode, childNode, directChildRel,
                 CASE WHEN directChildRel.subtreeTags IS NOT NULL
                      THEN apoc.convert.fromJsonMap(directChildRel.subtreeTags)
                      ELSE {} END as existingChildTags

            // Set the inherited tag on child nodes, preserving existing tags
            SET directChildRel.subtreeTags = apoc.convert.toJson(existingChildTags)

            RETURN count(*)',
                [
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'affectedDimensionSpacePointHashes' => $affectedDimensionSpacePointHashes,
                ]
            ));
    }
}
