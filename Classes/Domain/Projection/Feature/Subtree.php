<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\NodeQueryBuilder;
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
        $this->client->runStatement(
            Statement::create('
            MATCH (currentNode:Node {aggregateId: $aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()
            WHERE rel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes
            OPTIONAL MATCH (:Node)-[nestedRels:IS_CHILD*..]->(currentNode)-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()
            WHERE all(nestedRel IN nestedRels WHERE nestedRel.contentStreamId = $contentStreamId
            AND nestedRel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes)
            SET rel.subtreeTags = $subtreeTags
            WITH nestedRels
            UNWIND nestedRels AS nestedRel
            SET nestedRel.subtreeTags = $nestedSubtreeTags
            RETURN *',
            [
                'aggregateId' => $nodeAggregateId->value,
                'contentStreamId' => $contentStreamId->value,
                'affectedDimensionSpacePointHashes' => $affectedDimensionSpacePointHashes,
                'subtreeTags' => json_encode([$tag->value => true]),
                'nestedSubtreeTags' => json_encode([$tag->value => 'inherit']),
            ]
        ));
    }


    private function removeSubtreeTag(ContentStreamId $contentStreamId, NodeAggregateId $nodeAggregateId, DimensionSpacePointSet $affectedDimensionSpacePoints, SubtreeTag $tag): void
    {
        $affectedDimensionSpacePointHashes = $affectedDimensionSpacePoints->getPointHashes();
        $this->client->runStatement(
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
            
            // Handle child nodes
            WITH currentNode, updatedCurrentTags
            OPTIONAL MATCH (childNode)-[childRels:IS_CHILD*.. {contentStreamId: $contentStreamId}]->(currentNode)
            WHERE all(childRel IN childRels WHERE childRel.contentStreamId = $contentStreamId
            AND childRel.dimensionSpacePointHash IN $affectedDimensionSpacePointHashes)
            
            // Get the direct parent relationship for each child
            WITH currentNode, updatedCurrentTags, childNode, childRels,
                 childRels[size(childRels)-1] as directChildRel
            
            // Get child tags
            WITH currentNode, updatedCurrentTags, childNode, directChildRel,
                 CASE WHEN directChildRel.subtreeTags IS NOT NULL 
                      THEN apoc.convert.fromJsonMap(directChildRel.subtreeTags) 
                      ELSE {} END as childTags
            
            // Only remove tag from children if they have it as "inherit", not as true
            WITH currentNode, updatedCurrentTags, childNode, directChildRel, childTags,
                 CASE WHEN $tagValue IN keys(childTags) AND childTags[$tagValue] = "inherit"
                      THEN apoc.map.removeKey(childTags, $tagValue)
                      ELSE childTags END as updatedChildTags
            
            SET directChildRel.subtreeTags = CASE WHEN size(keys(updatedChildTags)) > 0 
                                                  THEN apoc.convert.toJson(updatedChildTags)
                                                  ELSE null END
            
            RETURN count(*)',
            [
                'aggregateId' => $nodeAggregateId->value,
                'contentStreamId' => $contentStreamId->value,
                'affectedDimensionSpacePointHashes' => $affectedDimensionSpacePointHashes,
                'tagValue' => $tag->value,
            ]
        ));
    }
}
