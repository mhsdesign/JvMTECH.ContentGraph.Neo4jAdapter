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

        foreach ($affectedDimensionSpacePointHashes as $dimensionSpacePointHash) {
            $this->client->runStatement(
                Statement::create('
                // Find the node and its parent relationship to get inherited tags
                MATCH (currentNode:Node {aggregateId: $aggregateId})-[currentRel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(parentNode)

                // Get parent\'s relationship to understand what tags should be inherited
                OPTIONAL MATCH (parentNode)-[parentRel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()

                // Get current and parent tags
                WITH currentNode, currentRel, parentRel,
                     CASE WHEN currentRel.subtreeTags IS NOT NULL
                          THEN apoc.convert.fromJsonMap(currentRel.subtreeTags)
                          ELSE {} END as currentTags,
                     CASE WHEN parentRel IS NOT NULL AND parentRel.subtreeTags IS NOT NULL
                          THEN apoc.convert.fromJsonMap(parentRel.subtreeTags)
                          ELSE {} END as parentTags

                // Convert parent tags to inherited tags (true -> inherit, inherit -> inherit)
                WITH currentNode, currentRel, currentTags, parentTags,
                     apoc.map.fromPairs([key in keys(parentTags) WHERE parentTags[key] IN [true, "inherit"] | [key, "inherit"]]) as inheritedTags

                // Filter out inherited tags that are no longer present on parent, keep explicit tags
                WITH currentNode, currentRel, currentTags, inheritedTags, parentTags,
                     apoc.map.fromPairs([key in keys(currentTags) WHERE currentTags[key] = true OR (currentTags[key] = "inherit" AND key IN keys(parentTags) AND parentTags[key] IN [true, "inherit"]) | [key, currentTags[key]]]) as filteredCurrentTags

                // Merge inherited tags with filtered current tags
                WITH currentNode, currentRel, filteredCurrentTags, inheritedTags,
                     apoc.map.merge(inheritedTags, filteredCurrentTags) as updatedTags

                // Update the relationship with proper inherited tags
                SET currentRel.subtreeTags = CASE WHEN size(keys(updatedTags)) > 0
                                                  THEN apoc.convert.toJson(updatedTags)
                                                  ELSE null END

                // Now handle all descendant nodes recursively
                WITH currentNode
                OPTIONAL MATCH (descendantNode)-[descendantRels:IS_CHILD*1.. {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(currentNode)

                // For each descendant, get its direct parent relationship
                WITH descendantNode, descendantRels,
                     descendantRels[0] as directDescendantRel

                // Find the direct parent node and its relationship
                WITH descendantNode, directDescendantRel,
                     endNode(directDescendantRel) as directParentNode

                OPTIONAL MATCH (directParentNode)-[directParentRel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()

                // Get descendant current tags and parent tags
                WITH descendantNode, directDescendantRel, directParentRel,
                     CASE WHEN directDescendantRel.subtreeTags IS NOT NULL
                          THEN apoc.convert.fromJsonMap(directDescendantRel.subtreeTags)
                          ELSE {} END as descendantCurrentTags,
                     CASE WHEN directParentRel IS NOT NULL AND directParentRel.subtreeTags IS NOT NULL
                          THEN apoc.convert.fromJsonMap(directParentRel.subtreeTags)
                          ELSE {} END as descendantParentTags

                // Convert parent tags to inherited tags for descendant
                WITH descendantNode, directDescendantRel, descendantCurrentTags, descendantParentTags,
                     apoc.map.fromPairs([key in keys(descendantParentTags) WHERE descendantParentTags[key] IN [true, "inherit"] | [key, "inherit"]]) as descendantInheritedTags

                // Filter out inherited tags that are no longer present on parent, keep explicit tags
                WITH descendantNode, directDescendantRel, descendantCurrentTags, descendantInheritedTags, descendantParentTags,
                     apoc.map.fromPairs([key in keys(descendantCurrentTags) WHERE descendantCurrentTags[key] = true OR (descendantCurrentTags[key] = "inherit" AND key IN keys(descendantParentTags) AND descendantParentTags[key] IN [true, "inherit"]) | [key, descendantCurrentTags[key]]]) as filteredDescendantCurrentTags

                // Merge inherited tags with filtered current explicit tags
                WITH descendantNode, directDescendantRel, filteredDescendantCurrentTags, descendantInheritedTags,
                     apoc.map.merge(descendantInheritedTags, filteredDescendantCurrentTags) as descendantUpdatedTags

                // Update descendant relationship with proper inherited tags
                SET directDescendantRel.subtreeTags = CASE WHEN size(keys(descendantUpdatedTags)) > 0
                                                           THEN apoc.convert.toJson(descendantUpdatedTags)
                                                           ELSE null END

                RETURN count(*)',
                    [
                        'aggregateId' => $nodeAggregateId->value,
                        'contentStreamId' => $contentStreamId->value,
                        'dimensionSpacePointHash' => $dimensionSpacePointHash,
                    ]
                )
            );
        }
    }
}
