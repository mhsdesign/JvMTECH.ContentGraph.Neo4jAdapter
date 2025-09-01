<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jProjectionContentGraph;
use JvMTECH\ContentGraph\Neo4jAdapter\Neo4jContentGraphProjection;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Types\Node;
use Laudis\Neo4j\Types\Relationship;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;

trait HierarchyRelation
{
    private readonly ClientInterface $client;
    private readonly Neo4jProjectionContentGraph $projectionContentGraph;
    private function addParentHierarchyRelation(
        Node $childNode,
        NodeAggregateId $parentNodeAggregateId,
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        int $position,
        \DateTimeImmutable $lastModified,
        \DateTimeImmutable $originalLastModified
    ): void {
        $this->client->runStatement(
            Statement::create(
                'MATCH (childNode:Node) WHERE ID(childNode) = $childNodeAggregateId
                    MATCH (parentNode:Node|Root {aggregateId: $parentNodeAggregateId})-[parentRel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    
                    // Get parent subtree tags and convert them to inherited tags for the child
                    WITH childNode, parentNode, parentRel,
                         CASE WHEN parentRel.subtreeTags IS NOT NULL 
                              THEN apoc.convert.fromJsonMap(parentRel.subtreeTags) 
                              ELSE {} END as parentTags
                    
                    // Convert parent tags to inherited tags for child (true -> inherit, inherit -> inherit)
                    WITH childNode, parentNode, parentRel, parentTags,
                         apoc.map.fromPairs([key in keys(parentTags) WHERE parentTags[key] IN [true, "inherit"] | [key, "inherit"]]) as inheritedTags
                    
                    CREATE (childNode)-[:IS_CHILD {
                        contentStreamId: $contentStreamId,
                        dimensionSpacePointHash: $dimensionSpacePointHash,
                        position: $position,
                        subtreeTags: CASE WHEN size(keys(inheritedTags)) > 0 
                                          THEN apoc.convert.toJson(inheritedTags)
                                          ELSE null END
                    }]->(parentNode)
                    SET childNode.lastModified = $lastModified
                    SET childNode.originalLastModified = $originalLastModified',
                [
                    'childNodeAggregateId' => $childNode->getId(),
                    'parentNodeAggregateId' => $parentNodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    'position' => $position,
                    'lastModified' => $lastModified->format(\DateTimeInterface::ATOM),
                    'originalLastModified' => $originalLastModified->format(\DateTimeInterface::ATOM),
                ]
            )
        );
    }

    private function addChildHierarchyRelation(): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (parentNode:Node) WHERE ID(parentNode) = $parentNodeAggregateId
                CREATE (parentNode)<-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash, position: $position}]-(childNode:Node)
                SET childNode.lastModified = $lastModified
                SET childNode.originalLastModified = $originalLastModified',
                [
                    'parentNodeAggregateId' => $parentNodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    'position' => Neo4jContentGraphProjection::RELATION_DEFAULT_OFFSET,
                    'lastModified' => $lastModified->format(\DateTimeInterface::ATOM),
                    'originalLastModified' => $originalLastModified->format(\DateTimeInterface::ATOM),
                ]
            )
        );
    }

    private function copyHierarchyRelation(
        Relationship $relationship,
        Node $newChildNode,
        Node $newParentNode,
        DimensionSpacePoint $dimensionSpacePoint,
        int $position,
        bool $copyDisabledState = true,
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH ()-[rOld]->() WHERE ID(rOld) = $relationshipId
                MATCH (newChildNode:Node)
                MATCH (newParentNode:Node)
                WHERE ID(newChildNode) = $newChildNodeId AND ID(newParentNode) = $newParentNodeId
                
                // Find parent node\'s relationship for tag inheritance using the same contentStreamId and dimensionSpacePointHash
                OPTIONAL MATCH (newParentNode)-[parentRel:IS_CHILD {contentStreamId: rOld.contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                
                // Get parent subtree tags and convert them to inherited tags for the child
                WITH rOld, newChildNode, newParentNode, parentRel,
                     CASE WHEN parentRel.subtreeTags IS NOT NULL 
                          THEN apoc.convert.fromJsonMap(parentRel.subtreeTags) 
                          ELSE {} END as parentTags,
                     CASE WHEN rOld.subtreeTags IS NOT NULL AND $copySubtreeTags = TRUE
                          THEN apoc.convert.fromJsonMap(rOld.subtreeTags)
                          ELSE {} END as oldTags
                
                // Convert parent tags to inherited tags for child (true -> inherit, inherit -> inherit)
                WITH rOld, newChildNode, newParentNode, parentTags, oldTags,
                     apoc.map.fromPairs([key in keys(parentTags) WHERE parentTags[key] IN [true, "inherit"] | [key, "inherit"]]) as inheritedTags
                
                // Merge inherited tags with old tags, with old tags taking precedence
                WITH rOld, newChildNode, newParentNode, inheritedTags, oldTags,
                     apoc.map.merge(inheritedTags, CASE WHEN $copyDisabledState = TRUE 
                                                        THEN oldTags
                                                        ELSE apoc.map.removeKey(oldTags, "disabled") END) as finalTags
                
                CREATE (newChildNode)-[rNew:IS_CHILD {
                    contentStreamId: rOld.contentStreamId,
                    dimensionSpacePointHash: $dimensionSpacePointHash,
                    position: $position,
                    subtreeTags: CASE WHEN size(keys(finalTags)) > 0 
                                      THEN apoc.convert.toJson(finalTags)
                                      ELSE null END
                }]->(newParentNode)',
                [
                    'relationshipId' => $relationship->getId(),
                    'newChildNodeId' => $newChildNode->getId(),
                    'newParentNodeId' => $newParentNode->getId(),
                    'position' => $position,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    'copySubtreeTags' => true,
                    'copyDisabledState' => $copyDisabledState,
                ]
            )
        );
    }

    private function moveChildHierarchyRelation(
        Node $newChildNode,
        Relationship $relationship,
        int $position,
        bool $copyDisabledState = true,
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (newChildNode:Node) WHERE ID(newChildNode) = $newChildNodeId
                MATCH ()-[relationship]->(parentNode) WHERE ID(relationship) = $relationshipId
                
                // Find parent node\'s relationship for tag inheritance using the same contentStreamId and dimensionSpacePointHash
                OPTIONAL MATCH (parentNode)-[parentRel:IS_CHILD {contentStreamId: relationship.contentStreamId, dimensionSpacePointHash: relationship.dimensionSpacePointHash}]->()
                
                // Get parent subtree tags and existing relationship tags
                WITH newChildNode, relationship, parentNode, parentRel,
                     CASE WHEN parentRel.subtreeTags IS NOT NULL 
                          THEN apoc.convert.fromJsonMap(parentRel.subtreeTags) 
                          ELSE {} END as parentTags,
                     CASE WHEN relationship.subtreeTags IS NOT NULL 
                          THEN apoc.convert.fromJsonMap(relationship.subtreeTags) 
                          ELSE {} END as existingTags
                
                // Convert parent tags to inherited tags for child (true -> inherit, inherit -> inherit)
                WITH newChildNode, relationship, parentNode, parentTags, existingTags,
                     apoc.map.fromPairs([key in keys(parentTags) WHERE parentTags[key] IN [true, "inherit"] | [key, "inherit"]]) as inheritedTags
                
                // Merge inherited tags with existing tags, with existing tags taking precedence
                WITH newChildNode, relationship, inheritedTags, existingTags,
                     apoc.map.merge(inheritedTags, CASE WHEN $copyDisabledState = TRUE 
                                                        THEN existingTags
                                                        ELSE apoc.map.removeKey(existingTags, "disabled") END) as finalTags
                
                CALL apoc.refactor.from(relationship, newChildNode)
                YIELD output as newRelationship
                SET newRelationship.position = $position,
                    newRelationship.subtreeTags = CASE WHEN size(keys(finalTags)) > 0 
                                                        THEN apoc.convert.toJson(finalTags)
                                                        ELSE null END',
                [
                    'newChildNodeId' => $newChildNode->getId(),
                    'relationshipId' => $relationship->getId(),
                    'position' => $position,
                    'copyDisabledState' => $copyDisabledState,
                ]
            )
        );
    }

    private function moveParentHierarchyRelation(
        Node $newParentNode,
        Relationship $relationship,
        int $position,
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (newParentNode:Node) WHERE ID(newParentNode) = $newParentNodeId
                MATCH ()-[relationship]->() WHERE ID(relationship) = $relationshipId
                CALL apoc.refactor.to(relationship, newParentNode)
                YIELD output as newRelationship
                SET newRelationship.position = $position',
                [
                    'newParentNodeId' => $newParentNode->getId(),
                    'relationshipId' => $relationship->getId(),
                    'position' => $position,
                ])
        );
    }
    private function addRootRelation(
        Node $childNode,
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        \DateTimeImmutable $lastModified,
        \DateTimeImmutable $originalLastModified
    ): void {
        $this->client->runStatement(
            Statement::create(
                'MATCH (childNode:Node) WHERE ID(childNode) = $childNodeAggregateId
                    MERGE (root:Root)
                    CREATE (childNode)-[:IS_CHILD {
                        contentStreamId: $contentStreamId,
                         dimensionSpacePointHash: $dimensionSpacePointHash,
                         position: $position
                     }]->(root)
                    SET childNode.lastModified = $lastModified
                    SET childNode.originalLastModified = $originalLastModified',
                [
                    'childNodeAggregateId' => $childNode->getId(),
                    'contentStreamId' => $contentStreamId->value,
                    'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    'position' => $this->projectionContentGraph->determineRootNodePosition(
                        $contentStreamId,
                        $dimensionSpacePoint,
                    ),
                    'lastModified' => $lastModified->format(\DateTimeInterface::ATOM),
                    'originalLastModified' => $originalLastModified->format(\DateTimeInterface::ATOM),
                ]
            )
        );
    }
}
