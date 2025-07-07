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
                CREATE (newChildNode)-[rNew:IS_CHILD {
                    contentStreamId: rOld.contentStreamId,
                    dimensionSpacePointHash: $dimensionSpacePointHash,
                    position: $position
                }]->(newParentNode)
                FOREACH (_ IN CASE WHEN rOld.subtreeTags IS NOT NULL AND $copySubtreeTags = TRUE THEN [1] ELSE [] END |
                    // Copy subtree tags conditionally based on copyDisabledState parameter
                    SET rNew.subtreeTags = CASE WHEN $copyDisabledState = TRUE 
                                                THEN rOld.subtreeTags
                                                ELSE CASE WHEN size(keys(apoc.map.removeKey(apoc.convert.fromJsonMap(rOld.subtreeTags), "disabled"))) > 0
                                                          THEN apoc.convert.toJson(apoc.map.removeKey(apoc.convert.fromJsonMap(rOld.subtreeTags), "disabled"))
                                                          ELSE null END END
                )',
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
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (newChildNode:Node) WHERE ID(newChildNode) = $newChildNodeId
                MATCH ()-[relationship]->() WHERE ID(relationship) = $relationshipId
                CALL apoc.refactor.from(relationship, newChildNode)
                YIELD output as newRelationship
                SET newRelationship.position = $position',
                [
                    'newChildNodeId' => $newChildNode->getId(),
                    'relationshipId' => $relationship->getId(),
                    'position' => $position,
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
