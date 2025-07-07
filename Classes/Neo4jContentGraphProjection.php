<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter;

use DateTimeImmutable;
use DateTimeInterface;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature\ContentStream;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature\HierarchyRelation;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature\ReferenceRelation;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature\Subtree;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature\Workspace;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\NodeQueryBuilder;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jDimensionSpacePointsRepository;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jProjectionContentGraph;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Laudis\Neo4j\Types\Node;
use Neos\ContentRepository\Core\DimensionSpace\OriginDimensionSpacePoint;
use Neos\ContentRepository\Core\EventStore\EventInterface;
use Neos\ContentRepository\Core\EventStore\InitiatingEventMetadata;
use Neos\ContentRepository\Core\Feature\Common\EmbedsContentStreamId;
use Neos\ContentRepository\Core\Feature\Common\PublishableToWorkspaceInterface;
use Neos\ContentRepository\Core\Feature\ContentStreamClosing\Event\ContentStreamWasClosed;
use Neos\ContentRepository\Core\Feature\ContentStreamClosing\Event\ContentStreamWasReopened;
use Neos\ContentRepository\Core\Feature\ContentStreamCreation\Event\ContentStreamWasCreated;
use Neos\ContentRepository\Core\Feature\ContentStreamEventStreamName;
use Neos\ContentRepository\Core\Feature\ContentStreamForking\Event\ContentStreamWasForked;
use Neos\ContentRepository\Core\Feature\ContentStreamRemoval\Event\ContentStreamWasRemoved;
use Neos\ContentRepository\Core\Feature\DimensionSpaceAdjustment\Event\DimensionShineThroughWasAdded;
use Neos\ContentRepository\Core\Feature\NodeCreation\Event\NodeAggregateWithNodeWasCreated;
use Neos\ContentRepository\Core\Feature\NodeModification\Event\NodePropertiesWereSet;
use Neos\ContentRepository\Core\Feature\NodeMove\Event\NodeAggregateWasMoved;
use Neos\ContentRepository\Core\Feature\NodeReferencing\Event\NodeReferencesWereSet;
use Neos\ContentRepository\Core\Feature\NodeRemoval\Event\NodeAggregateWasRemoved;
use Neos\ContentRepository\Core\Feature\NodeRenaming\Event\NodeAggregateNameWasChanged;
use Neos\ContentRepository\Core\Feature\NodeTypeChange\Event\NodeAggregateTypeWasChanged;
use Neos\ContentRepository\Core\Feature\NodeVariation\Event\NodeGeneralizationVariantWasCreated;
use Neos\ContentRepository\Core\Feature\NodeVariation\Event\NodePeerVariantWasCreated;
use Neos\ContentRepository\Core\Feature\NodeVariation\Event\NodeSpecializationVariantWasCreated;
use Neos\ContentRepository\Core\Feature\RootNodeCreation\Event\RootNodeAggregateDimensionsWereUpdated;
use Neos\ContentRepository\Core\Feature\RootNodeCreation\Event\RootNodeAggregateWithNodeWasCreated;
use Neos\ContentRepository\Core\Feature\SubtreeTagging\Event\SubtreeWasTagged;
use Neos\ContentRepository\Core\Feature\SubtreeTagging\Event\SubtreeWasUntagged;
use Neos\ContentRepository\Core\Feature\WorkspaceCreation\Event\RootWorkspaceWasCreated;
use Neos\ContentRepository\Core\Feature\WorkspaceCreation\Event\WorkspaceWasCreated;
use Neos\ContentRepository\Core\Feature\WorkspaceModification\Event\WorkspaceBaseWorkspaceWasChanged;
use Neos\ContentRepository\Core\Feature\WorkspaceModification\Event\WorkspaceWasRemoved;
use Neos\ContentRepository\Core\Feature\WorkspacePublication\Event\WorkspaceWasDiscarded;
use Neos\ContentRepository\Core\Feature\WorkspacePublication\Event\WorkspaceWasPublished;
use Neos\ContentRepository\Core\Feature\WorkspaceRebase\Event\WorkspaceRebaseFailed;
use Neos\ContentRepository\Core\Feature\WorkspaceRebase\Event\WorkspaceWasRebased;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphProjectionInterface;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphReadModelInterface;
use Neos\ContentRepository\Core\Projection\ProjectionStatus;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\EventStore\Model\EventEnvelope;
use SebastianBergmann\CodeCoverage\Report\Xml\Project;

class Neo4jContentGraphProjection implements ContentGraphProjectionInterface
{
    use ContentStream, HierarchyRelation, ReferenceRelation, Workspace, Subtree;

    protected array $requiredIndexes = [
        [
            'name' => 'node_aggregateid',
            'state' => 'ONLINE',
            'type' => 'RANGE',
            'entityType' => 'NODE',
            'labelsOrTypes' => ['Node'],
            'properties' => ['aggregateId'],
        ],
        [
            'name' => 'node_nodetypename',
            'state' => 'ONLINE',
            'type' => 'RANGE',
            'entityType' => 'NODE',
            'labelsOrTypes' => ['Node'],
            'properties' => ['nodeTypeName'],
        ],
        [
            'name' => 'rel_contentstreamid',
            'state' => 'ONLINE',
            'type' => 'RANGE',
            'entityType' => 'RELATIONSHIP',
            'labelsOrTypes' => ['IS_CHILD'],
            'properties' => ['contentStreamId'],
        ],
        [
            'name' => 'rel_dimensionspacepointhash',
            'state' => 'ONLINE',
            'type' => 'RANGE',
            'entityType' => 'RELATIONSHIP',
            'labelsOrTypes' => ['IS_CHILD'],
            'properties' => ['dimensionSpacePointHash'],
        ]
    ];
    public const RELATION_DEFAULT_OFFSET = 128;

    public function __construct(
        private readonly ClientInterface $client,
        private readonly ContentGraphReadModelInterface $contentGraphReadModel,
        private readonly Neo4jDimensionSpacePointsRepository $dimensionSpacePointsRepository,
        private readonly Neo4jProjectionContentGraph $projectionContentGraph,
    ) {
    }

    public function inSimulation(\Closure $fn): mixed
    {
        // TODO: Implement inSimulation() method.
        return $fn();
    }

    public function getState(): ContentGraphReadModelInterface
    {
        return $this->contentGraphReadModel;
    }

    public function setUp(): void
    {
        $indexes = $this->client->run('SHOW INDEXES');
        foreach ($indexes as $index) {
            $this->client->runStatement(
                Statement::create('DROP INDEX ' . $index['name'] . ' IF EXISTS')
            );
        }
        foreach ($this->requiredIndexes as $requiredIndex) {
            $this->client->runStatement(
                Statement::create(sprintf(
                    'CREATE INDEX %s FOR (node:%s) ON node.%s',
                    $requiredIndex['name'],
                    $requiredIndex['entityType'],
                    $requiredIndex['properties'][0]
                ))
            );
        }
    }



    public function apply(EventInterface $event, EventEnvelope $eventEnvelope): void
    {
        match ($event::class) {
            ContentStreamWasClosed::class => $this->whenContentStreamWasClosed($event),
            ContentStreamWasCreated::class => $this->whenContentStreamWasCreated($event),
            ContentStreamWasForked::class => $this->whenContentStreamWasForked($event),
            ContentStreamWasRemoved::class => $this->whenContentStreamWasRemoved($event),
            ContentStreamWasReopened::class => $this->whenContentStreamWasReopened($event),
            DimensionShineThroughWasAdded::class => $this->whenDimensionShineThroughWasAdded($event),
//            DimensionSpacePointWasMoved::class => $this->whenDimensionSpacePointWasMoved($event),
            NodeAggregateNameWasChanged::class => $this->whenNodeAggregateNameWasChanged($event, $eventEnvelope),
            NodeAggregateTypeWasChanged::class => $this->whenNodeAggregateTypeWasChanged($event, $eventEnvelope),
            NodeAggregateWasMoved::class => $this->whenNodeAggregateWasMoved($event),
            NodeAggregateWasRemoved::class => $this->whenNodeAggregateWasRemoved($event),
            NodeAggregateWithNodeWasCreated::class => $this->whenNodeAggregateWithNodeWasCreated($event, $eventEnvelope),
            NodeGeneralizationVariantWasCreated::class => $this->whenNodeGeneralizationVariantWasCreated($event, $eventEnvelope),
            NodePeerVariantWasCreated::class => $this->whenNodePeerVariantWasCreated($event, $eventEnvelope),
            NodePropertiesWereSet::class => $this->whenNodePropertiesWereSet($event, $eventEnvelope),
            NodeReferencesWereSet::class => $this->whenNodeReferencesWereSet($event, $eventEnvelope),
            NodeSpecializationVariantWasCreated::class => $this->whenNodeSpecializationVariantWasCreated($event, $eventEnvelope),
            RootNodeAggregateDimensionsWereUpdated::class => $this->whenRootNodeAggregateDimensionsWereUpdated($event),
            RootNodeAggregateWithNodeWasCreated::class => $this->whenRootNodeAggregateWithNodeWasCreated($event, $eventEnvelope),
            RootWorkspaceWasCreated::class => $this->whenRootWorkspaceWasCreated($event),
            SubtreeWasTagged::class => $this->whenSubtreeWasTagged($event),
            SubtreeWasUntagged::class => $this->whenSubtreeWasUntagged($event),
            WorkspaceBaseWorkspaceWasChanged::class => $this->whenWorkspaceBaseWorkspaceWasChanged($event),
            WorkspaceRebaseFailed::class => $this->whenWorkspaceRebaseFailed($event),
            WorkspaceWasCreated::class => $this->whenWorkspaceWasCreated($event),
            WorkspaceWasDiscarded::class => $this->whenWorkspaceWasDiscarded($event),
            WorkspaceWasPublished::class => $this->whenWorkspaceWasPublished($event),
            WorkspaceWasRebased::class => $this->whenWorkspaceWasRebased($event),
            WorkspaceWasRemoved::class => $this->whenWorkspaceWasRemoved($event),
            default => null,
        };
        if (
            $event instanceof EmbedsContentStreamId
            && ContentStreamEventStreamName::isContentStreamStreamName($eventEnvelope->streamName)
            && !(
                // special case as we dont need to update anything. The handling above takes care of setting the version to 0
                $event instanceof ContentStreamWasForked
                || $event instanceof ContentStreamWasCreated
            )
        ) {
            $this->updateContentStreamVersion($event->getContentStreamId(), $eventEnvelope->version,
                $event instanceof PublishableToWorkspaceInterface);
        }
    }

    private function whenContentStreamWasClosed(ContentStreamWasClosed $event): void
    {
        $this->closeContentStream($event->getContentStreamId());
    }

    private function whenContentStreamWasCreated(ContentStreamWasCreated $event): void
    {
        $this->createContentStream($event->getContentStreamId());
    }

    private function whenContentStreamWasForked(ContentStreamWasForked $event): void
    {
        $this->createContentStream($event->newContentStreamId, $event->sourceContentStreamId,
            $event->versionOfSourceContentStream);
        // Update nodes that have a IS_CHILD relationships in the source content stream, add additional relationships to the new content stream
        $this->client->runStatement(
            Statement::create(
                'MATCH (childNode:Node)-[r:IS_CHILD {contentStreamId: $sourceContentStreamId}]->(parentNode:Node|Root)
                MERGE (childNode)-[rNew:IS_CHILD {dimensionSpacePointHash: r.dimensionSpacePointHash, contentStreamId: $targetContentStreamId, position: r.position}]->(parentNode)
                 FOREACH (_ IN CASE WHEN r.subtreeTags IS NOT NULL THEN [1] ELSE [] END |
                      SET rNew.subtreeTags = r.subtreeTags
                  )',
                [
                    'sourceContentStreamId' => $event->sourceContentStreamId->value,
                    'targetContentStreamId' => $event->newContentStreamId->value,
                ],
            )
        );
    }

    private function whenContentStreamWasRemoved(ContentStreamWasRemoved $event): void
    {
        // Remove all IS_CHILD relationships in the source content stream
        $this->client->runStatement(
            Statement::create(
                'MATCH (:Node)-[r:IS_CHILD {contentStreamId: $contentStreamId}]->(:Node|Root) DELETE r',
                [
                    'contentStreamId' => $event->contentStreamId->value,
                ],
            )
        );
        // Remove all nodes that dont have a IS_CHILD relationships anymore
        $this->client->runStatement(
            Statement::create(
                'MATCH (childNode:Node) WHERE NOT (childNode)-[:IS_CHILD]->() DETACH DELETE childNode',
            )
        );
        $this->removeContentStream($event->getContentStreamId());
    }

    private function whenContentStreamWasReopened(ContentStreamWasReopened $event): void
    {
        $this->reopenContentStream($event->getContentStreamId());
    }

    private function whenDimensionShineThroughWasAdded(DimensionShineThroughWasAdded $event): void
    {
        $this->dimensionSpacePointsRepository->insertDimensionSpacePoint($event->target);
        // Copy all hierarchy relations from source dimension space point to target dimension space point
        // This makes content "shine through" from the source to the target dimension space point
        $this->client->runStatement(
            Statement::create(
                '
                MATCH (childNode:Node)-[r:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $sourceDimensionSpacePointHash}]->(parentNode:Node|Root)
                CREATE (childNode)-[rNew:IS_CHILD {contentStreamId: r.contentStream, dimensionSpacePointHash: $targetDimensionSpacePointHash, position: r.position}]->(parentNode)
                FOREACH (_ IN CASE WHEN r.subtreeTags IS NOT NULL THEN [1] ELSE [] END |
                      SET rNew.subtreeTags = r.subtreeTags
                  )
                ',
                [
                    'contentStreamId' => $event->getContentStreamId()->value,
                    'sourceDimensionSpacePointHash' => $event->source->hash,
                    'targetDimensionSpacePointHash' => $event->target->hash,
                ]
            )
        );
    }

    private function whenNodeAggregateNameWasChanged(NodeAggregateNameWasChanged $event, EventEnvelope $eventEnvelope): void
    {
        $affectedNodesInContentStream = $this->client->runStatement(
            Statement::create(
                'MATCH (node:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId}]-() RETURN DISTINCT node',
                [
                    'aggregateId' => $event->nodeAggregateId->value,
                    'contentStreamId' => $event->contentStreamId->value,
                ]
            )
        );
        foreach ($affectedNodesInContentStream as $affectedNodeCypher) {
            $affectedNode = $affectedNodeCypher->getAsNode('node');
            $nodeContentStreams = $this->client->runStatement(
                Statement::create(
                    'MATCH (n)-[rel:IS_CHILD]->() WHERE ID(n) = $nodeId RETURN DISTINCT rel.contentStreamId as contentStreamId',
                    [
                        'nodeId' => $affectedNode->getId(),
                    ]
                ),
            );
            if (count($nodeContentStreams) > 1) {
                $affectedNode = $this->cloneNode($affectedNode, $event->contentStreamId);
            }
            $this->client->runStatement(
                Statement::create(
                    'MATCH (n) WHERE ID(n) = $nodeId
                    SET n.name = $name
                    SET n.lastModified = $lastModified
                    SET n.originalLastModified = $originalLastModified',
                    [
                        'nodeId' => $affectedNode->getId(),
                        'name' => $event->newNodeName->value,
                        'lastModified' => $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM),
                        'originalLastModified' => self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    ]
                )
            );
        }
    }

    private function whenNodeAggregateTypeWasChanged(NodeAggregateTypeWasChanged $event, EventEnvelope $eventEnvelope): void
    {
        // TODO: Use correct copy on write handling here
        $affectedNodesInContentStream = $this->client->runStatement(
            Statement::create(
                'MATCH (node:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId}]-() RETURN DISTINCT node',
                [
                    'aggregateId' => $event->nodeAggregateId->value,
                    'contentStreamId' => $event->contentStreamId->value,
                ]
            )
        );
        foreach ($affectedNodesInContentStream as $affectedNodeCypher) {
            $affectedNode = $affectedNodeCypher->getAsNode('node');
            $nodeContentStreams = $this->client->runStatement(
                Statement::create(
                    'MATCH (n)-[rel:IS_CHILD]->() WHERE ID(n) = $nodeId RETURN DISTINCT rel.contentStreamId as contentStreamId',
                    [
                        'nodeId' => $affectedNode->getId(),
                    ]
                ),
            );
            if (count($nodeContentStreams) > 1) {
                $affectedNode = $this->cloneNode($affectedNode, $event->contentStreamId);
            }
            $this->client->runStatement(
                Statement::create(
                    'MATCH (n) WHERE ID(n) = $nodeId
                    SET n.nodeTypeName = $nodeTypeName
                    SET n.lastModified = $lastModified
                    SET n.originalLastModified = $originalLastModified',
                    [
                        'nodeId' => $affectedNode->getId(),
                        'nodeTypeName' => $event->newNodeTypeName->value,
                        'lastModified' => $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM),
                        'originalLastModified' => self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    ]
                )
            );
        }
    }

    private function whenNodeAggregateWasMoved(NodeAggregateWasMoved $event): void
    {
        foreach ($event->succeedingSiblingsForCoverage as $succeedingSiblingForCoverage) {
            $nodesToBeMovedResult = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForSubgraph(
                        $event->contentStreamId,
                        $succeedingSiblingForCoverage->dimensionSpacePoint,
                        $event->nodeAggregateId,
                        'node'
                    )
                    ->returns('node')
                    ->build()
            );
            if (!$nodesToBeMovedResult->hasKey(0) || !$nodesToBeMovedResult->getAsCypherMap(0)->hasKey('node')) {
                throw new \RuntimeException(sprintf('Failed to move node "%s" in sub graph %s@%s because it does not exist',
                    $event->nodeAggregateId->value, $succeedingSiblingForCoverage->dimensionSpacePoint->toJson(),
                    $event->contentStreamId->value), 1750063392);
            }
            if ($event->newParentNodeAggregateId) {
                $this->client->runStatement(
                    NodeQueryBuilder::createForNodes()
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $succeedingSiblingForCoverage->dimensionSpacePoint,
                            $event->nodeAggregateId,
                        )
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $succeedingSiblingForCoverage->dimensionSpacePoint,
                            $event->newParentNodeAggregateId,
                            'newParent',
                            '',
                            '',
                        )
                        ->call('apoc.refactor.to(rel, newParent)')
                        ->yield('output as newParentRel')
                        ->setProperty('position', $this->projectionContentGraph->determineHierarchyRelationPosition(
                            parentAggregateId: $event->newParentNodeAggregateId,
                            succeedingSiblingAggregateId: $succeedingSiblingForCoverage->nodeAggregateId,
                            contentStreamId: $event->contentStreamId,
                            dimensionSpacePoint: $succeedingSiblingForCoverage->dimensionSpacePoint
                        ), 'newParentRel')
                        ->build()
                );
            } else if ($succeedingSiblingForCoverage->nodeAggregateId) {
                $this->client->runStatement(
                    NodeQueryBuilder::createForNodes()
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $succeedingSiblingForCoverage->dimensionSpacePoint,
                            $succeedingSiblingForCoverage->nodeAggregateId,
                            'sibling',
                            '',
                            '',
                        )
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $succeedingSiblingForCoverage->dimensionSpacePoint,
                            $event->nodeAggregateId,
                        )
                        ->setProperty('position', $this->projectionContentGraph->determineHierarchyRelationPosition(
                            parentAggregateId: null,
                            succeedingSiblingAggregateId: $event->nodeAggregateId,
                            contentStreamId: $event->contentStreamId,
                            dimensionSpacePoint: $succeedingSiblingForCoverage->dimensionSpacePoint
                        ), 'rel')
                        ->returns('*')
                        ->build()
                );
            } else {
                $result = $this->client->runStatement(
                    $statement = NodeQueryBuilder::createForNodes()
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $succeedingSiblingForCoverage->dimensionSpacePoint,
                            $event->nodeAggregateId,
                        )
                        ->matchParentNodeForSubgraph(
                            $event->contentStreamId,
                            $succeedingSiblingForCoverage->dimensionSpacePoint,
                            $event->nodeAggregateId,
                            'parent',
                        )
                        ->returns('parent, rel')
                        ->build()
                );
                if (!$result->hasKey(0) || !$result->getAsCypherMap(0)->hasKey('parent')) {
                    throw new \RuntimeException(sprintf(
                        'Failed to move node "%s" in sub graph %s@%s because it does not have a parent',
                        $event->nodeAggregateId->value, $succeedingSiblingForCoverage->dimensionSpacePoint->toJson(),
                        $event->contentStreamId->value), 1750079649);
                }
                $this->client->runStatement(
                    Statement::create(
                        'MATCH ()-[rel]->() WHERE ID(rel) = $relId
                        SET rel.position = $position
                        ',
                        [
                            'relId' => $result->getAsCypherMap(0)->getAsRelationship('rel')->getId(),
                            'position' => $this->projectionContentGraph->determineHierarchyRelationPosition(
                                parentAggregateId: NodeAggregateId::fromString($result->getAsCypherMap(0)->getAsNode('parent')->getProperty('aggregateId')),
                                succeedingSiblingAggregateId: null,
                                contentStreamId: $event->contentStreamId,
                                dimensionSpacePoint: $succeedingSiblingForCoverage->dimensionSpacePoint
                            ),
                        ]
                    ),
                );

                // FIGURE OUT WHAT TO DO: Find topmost node in parent and place as last child == highest position + default
            }
        }
    }

    private function cloneNode(Node $node, ContentStreamId $contentStreamId): Node
    {
        // @see DoctrineDbalContentGraphProjection::updateNodeRecordWithCopyOnWrite
        $newNodeResult = $this->client->runStatement(
            Statement::create(
                '
                        MATCH (n:Node) WHERE ID(n) = $nodeId
                        CALL apoc.refactor.cloneNodes([n], true)
                        YIELD output as newNode
                        return newNode',
                [
                    'nodeId' => $node->getId(),
                ]
            )
        );
        $this->client->runStatement(
            Statement::create('
                MATCH (n:Node) WHERE ID(n) = $nodeId
                MATCH (newNode:Node) WHERE ID(newNode) = $newNodeId
                OPTIONAL MATCH (n)-[wrongOldRelation:IS_CHILD]-() WHERE wrongOldRelation.contentStreamId = $contentStreamId
                DELETE wrongOldRelation
                WITH newNode
                OPTIONAL MATCH (newNode)-[wrongRelations:IS_CHILD]-() WHERE wrongRelations.contentStreamId <> $contentStreamId
                DELETE wrongRelations',
                [
                    'nodeId' => $node->getId(),
                    'newNodeId' => $newNodeResult->getAsCypherMap(0)->getAsNode('newNode')->getId(),
                    'contentStreamId' => $contentStreamId->value,
                ]
            )
        );
        return $newNodeResult->getAsCypherMap(0)->getAsNode('newNode');
    }

    private function whenNodeAggregateWasRemoved(NodeAggregateWasRemoved $event): void
    {
        // First, delete outgoing IS_CHILD relationships for the node and all its descendants
        // in the given contentStreamId and covered dimension space points
        foreach ($event->affectedCoveredDimensionSpacePoints as $dimensionSpacePoint) {
            $this->client->runStatement(
                Statement::create(
                    'MATCH (node:Node {aggregateId: $aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    DELETE rel
                    WITH node
                    MATCH (node)<-[:IS_CHILD*0..]-(descendant:Node)
                    MATCH (descendant)-[descendantRel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                    DELETE descendantRel',
                    [
                        'aggregateId' => $event->nodeAggregateId->value,
                        'contentStreamId' => $event->contentStreamId->value,
                        'dimensionSpacePointHash' => $dimensionSpacePoint->hash,
                    ]
                )
            );
        }

        // Second, delete all nodes in the subtree that no longer have any IS_CHILD relationships
        $this->client->runStatement(
            Statement::create(
                'MATCH (node:Node)
                WHERE NOT (node)-[:IS_CHILD]->()
                DETACH DELETE node'
            )
        );
    }

    private function whenNodeAggregateWithNodeWasCreated(
        NodeAggregateWithNodeWasCreated $event,
        EventEnvelope $eventEnvelope
    ): void {
        // Get all covered dimension space points
        $this->dimensionSpacePointsRepository->insertDimensionSpacePoint($event->originDimensionSpacePoint);
        // Create the node once
        $result = $this->client->runStatement(
            Statement::create(
                'CREATE (node:Node {
                    aggregateId: $aggregateId,
                    nodeTypeName: $nodeTypeName,
                    classification: $classification,
                    name: $name,
                    originDimensionSpacePointHash: $originDimensionSpacePointHash,
                    created: $created,
                    originalCreated: $originalCreated,
                    lastModified: $lastModified,
                    originalLastModified: $originalLastModified,
                    properties: $properties
                }) RETURN node',
                [
                    'aggregateId' => $event->getNodeAggregateId()->value,
                    'nodeTypeName' => $event->nodeTypeName->value,
                    'classification' => $event->nodeAggregateClassification->value,
                    'name' => $event->nodeName?->value,
                    'originDimensionSpacePointHash' => $event->originDimensionSpacePoint->hash,
                    'created' => $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM),
                    'originalCreated' => self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    'lastModified' => null,
                    'originalLastModified' => null,
                    'properties' => json_encode($event->initialPropertyValues->jsonSerialize()),
                ]
            )
        );
        $newlyCreatedNode = $result->getAsCypherMap(0)->getAsNode('node');

        $this->createReferenceRelations(
            $event->nodeAggregateId,
            $event->contentStreamId,
            $event->originDimensionSpacePoint->toDimensionSpacePoint(),
            $event->nodeReferences,
            $eventEnvelope->recordedAt,
            self::initiatingDateTime($eventEnvelope)
        );

        $missingParentRelations = $event->succeedingSiblingsForCoverage->toDimensionSpacePointSet()->points;

        foreach ($missingParentRelations as $dimensionSpacePoint) {
            $succeedingSiblingNodeAggregateId = $event->succeedingSiblingsForCoverage->getSucceedingSiblingIdForDimensionSpacePoint($dimensionSpacePoint);

        }
        foreach ($event->succeedingSiblingsForCoverage as $sibling) {
            $this->addParentHierarchyRelation(
                $newlyCreatedNode,
                $event->parentNodeAggregateId,
                $event->contentStreamId,
                $sibling->dimensionSpacePoint,
                $this->projectionContentGraph->determineHierarchyRelationPosition(
                    parentAggregateId: $event->parentNodeAggregateId,
                    succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                    contentStreamId: $event->contentStreamId,
                    dimensionSpacePoint: $sibling->dimensionSpacePoint,
                ),
                $eventEnvelope->recordedAt,
                self::initiatingDateTime($eventEnvelope)
            );
        }
    }

    // DONE!
    private function whenNodeGeneralizationVariantWasCreated(
        NodeGeneralizationVariantWasCreated $event,
        EventEnvelope $eventEnvelope
    ): void {
        $nodeCloneResults = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph(
                    $event->contentStreamId,
                    $event->sourceOrigin->toDimensionSpacePoint(),
                    $event->nodeAggregateId,
                )
                ->call('apoc.refactor.cloneNodes([n], true)')
                ->yield('output AS generalizedNode')
                ->setProperty('originDimensionSpacePointHash', $event->generalizationOrigin->toDimensionSpacePoint()->hash,
                    'generalizedNode')
                ->setProperty('created', $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM), 'generalizedNode')
                ->setProperty('originalCreated', self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    'generalizedNode')
                ->with('generalizedNode, n, p, rel')
                ->optionalMatch('(generalizedNode)-[generalizedRel:IS_CHILD]-() DELETE generalizedRel')
                ->returns('*')
                ->build()
        );

        if (empty($nodeCloneResults->getAsCypherMap(0)) || empty($nodeCloneResults->getAsCypherMap(0)->get('p'))) {
            throw new \RuntimeException(sprintf('Failed to create node generalization variant for node "%s" in sub graph %s@%s because the source parent node is missing',
                $event->nodeAggregateId->value, $event->sourceOrigin->toJson(), $event->contentStreamId->value), 1749910013);
        }
        if (empty($nodeCloneResults->getAsCypherMap(0)) || empty($nodeCloneResults->getAsCypherMap(0)->get('n'))) {
            throw new \RuntimeException(sprintf('Failed to create node generalization variant for node "%s" in sub graph %s@%s because the source node is missing',
                $event->nodeAggregateId->value, $event->sourceOrigin->toJson(), $event->contentStreamId->value), 1749910075);
        }

        $sourceRelationship = $nodeCloneResults->getAsCypherMap(0)->getAsRelationship('rel');
        $this->copyReferenceRelations(
            $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
            $nodeCloneResults->getAsCypherMap(0)->getAsNode('generalizedNode')
        );

        $sourceParentNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('p');
        $generalizedNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('generalizedNode');
        // Find all ingoing outgoing relationships (node is source of IS_CHILD) and change the source to the generalized node that are in the given dsp set
        $unassignedIngoingDimensionSpacePoints = [];
        $variantSucceedingSiblings = $event->variantSucceedingSiblings;
        foreach ($variantSucceedingSiblings as $sibling) {
            $existingChildRelationships = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForSubgraph(
                        $event->contentStreamId,
                        $sibling->dimensionSpacePoint,
                        $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
                    )
                    ->returns('n, rel')
                    ->build()
            );
            if ($existingChildRelationships->isEmpty()) {
                $unassignedIngoingDimensionSpacePoints[] = $sibling->dimensionSpacePoint;
            }
            foreach ($existingChildRelationships as $existingChildRelationship) {
                $this->moveChildHierarchyRelation(
                    $generalizedNode,
                    $existingChildRelationship->getAsRelationship('rel'),
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($nodeCloneResults->getAsCypherMap(0)->getAsNode('p')->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $sibling->dimensionSpacePoint,
                    ),
                );
            }
            $existingParentRelationships = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchChildrenForSubgraph(
                        $event->contentStreamId,
                        $sibling->dimensionSpacePoint,
                        $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
                    )
                    ->returns('child, rel')
                    ->build()
            );
            foreach ($existingParentRelationships as $existingParentRelationship) {
                $this->moveParentHierarchyRelation(
                    $generalizedNode,
                    $existingParentRelationship->getAsRelationship('rel'),
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($nodeCloneResults->getAsCypherMap(0)->getAsNode('p')->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $sibling->dimensionSpacePoint,
                    ),
                );
            }
        }

        if (count($unassignedIngoingDimensionSpacePoints) > 0) {
            foreach ($unassignedIngoingDimensionSpacePoints as $unassignedIngoingDimensionSpacePoint) {
                $generalizationParentNodeResult = $this->client->runStatement(
                    NodeQueryBuilder::createForNodes()
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $unassignedIngoingDimensionSpacePoint,
                            $sourceParentNode,
                            nodeAlias: 'generalizationParentNode'
                        )
                        ->returns('DISTINCT generalizationParentNode')
                        ->build()
                );
                try {
                    $generalizationParentNode = $generalizationParentNodeResult->getAsCypherMap(0)->getAsNode('generalizationParentNode');
                } catch (\OutOfBoundsException) {
                    // TODO: throw correctly!
                    throw new \Exception('wrong', 1750009438);
                }
                $generalizationSucceedingSiblingNodeAggregateId = $variantSucceedingSiblings
                    ->getSucceedingSiblingIdForDimensionSpacePoint($unassignedIngoingDimensionSpacePoint);
                // if ($eventEnvelope->sequenceNumber->equals(SequenceNumber::fromInteger(7))) return;

                $this->copyHierarchyRelation(
                    $sourceRelationship,
                    $generalizedNode,
                    $generalizationParentNode,
                    $unassignedIngoingDimensionSpacePoint,
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($generalizationParentNode->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $generalizationSucceedingSiblingNodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $unassignedIngoingDimensionSpacePoint
                    ),
                );
            }
        }
    }

    // DONE!
    private function whenNodePeerVariantWasCreated(NodePeerVariantWasCreated $event, EventEnvelope $eventEnvelope): void
    {
        $nodeCloneResults = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph(
                    $event->contentStreamId,
                    $event->sourceOrigin->toDimensionSpacePoint(),
                    $event->nodeAggregateId,
                )
                ->call('apoc.refactor.cloneNodes([n], true)')
                ->yield('output AS peerNode')
                ->setProperty('originDimensionSpacePointHash', $event->peerOrigin->toDimensionSpacePoint()->hash,
                    'peerNode')
                ->setProperty('created', $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM), 'peerNode')
                ->setProperty('originalCreated', self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    'peerNode')
                ->with('peerNode, n, p, rel')
                ->optionalMatch('(peerNode)-[generalizedRel:IS_CHILD]-() DELETE generalizedRel')
                ->returns('*')
                ->build()
        );

        if (empty($nodeCloneResults->getAsCypherMap(0)) || empty($nodeCloneResults->getAsCypherMap(0)->get('p'))) {
            throw new \RuntimeException(sprintf('Failed to create node generalization variant for node "%s" in sub graph %s@%s because the source parent node is missing',
                $event->nodeAggregateId->value, $event->sourceOrigin->toJson(), $event->contentStreamId->value), 1749910013);
        }
        if (empty($nodeCloneResults->getAsCypherMap(0)) || empty($nodeCloneResults->getAsCypherMap(0)->get('n'))) {
            throw new \RuntimeException(sprintf('Failed to create node generalization variant for node "%s" in sub graph %s@%s because the source node is missing',
                $event->nodeAggregateId->value, $event->sourceOrigin->toJson(), $event->contentStreamId->value), 1749910075);
        }

        $sourceRelationship = $nodeCloneResults->getAsCypherMap(0)->getAsRelationship('rel');

        $peerNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('peerNode');
        $this->client->runStatement(
            Statement::create(
                'MATCH (peerNode) WHERE ID(peerNode) = $peerNodeId
                MATCH (peerNode)-[ref:REFERENCE]->(oldReferenceTarget)
                OPTIONAL MATCH (newReferenceTarget {aggregateId: oldReferenceTarget.aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                CALL apoc.refactor.to(ref, newReferenceTarget)
                YIELD output
                FINISH',
                [
                    'peerNodeId' => $peerNode->getId(),
                    'contentStreamId' => $event->contentStreamId->value,
                    'dimensionSpacePointHash' => $event->peerOrigin->hash,
                ]
            )
        );

        $sourceParentNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('p');
        $peerNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('peerNode');
        // Find all ingoing outgoing relationships (node is source of IS_CHILD) and change the source to the generalized node that are in the given dsp set
        $unassignedIngoingDimensionSpacePoints = [];
        $variantSucceedingSiblings = $event->peerSucceedingSiblings;
        foreach ($variantSucceedingSiblings as $sibling) {
            $existingChildRelationships = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForSubgraph(
                        $event->contentStreamId,
                        $sibling->dimensionSpacePoint,
                        $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
                    )
                    ->returns('n, rel')
                    ->build()
            );
            if ($existingChildRelationships->isEmpty()) {
                $unassignedIngoingDimensionSpacePoints[] = $sibling->dimensionSpacePoint;
            }
            foreach ($existingChildRelationships as $existingChildRelationship) {
                $this->moveChildHierarchyRelation(
                    $peerNode,
                    $existingChildRelationship->getAsRelationship('rel'),
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($nodeCloneResults->getAsCypherMap(0)->getAsNode('p')->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $sibling->dimensionSpacePoint,
                    ),
                );
            }
            $existingParentRelationships = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchChildrenForSubgraph(
                        $event->contentStreamId,
                        $sibling->dimensionSpacePoint,
                        $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
                    )
                    ->returns('child, rel')
                    ->build()
            );
            foreach ($existingParentRelationships as $existingParentRelationship) {
                $this->moveParentHierarchyRelation(
                    $peerNode,
                    $existingParentRelationship->getAsRelationship('rel'),
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($nodeCloneResults->getAsCypherMap(0)->getAsNode('p')->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $sibling->dimensionSpacePoint,
                    ),
                );
            }
        }

        if (count($unassignedIngoingDimensionSpacePoints) > 0) {
            foreach ($unassignedIngoingDimensionSpacePoints as $unassignedIngoingDimensionSpacePoint) {
                $peerParentNodeResult = $this->client->runStatement(
                    NodeQueryBuilder::createForNodes()
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $unassignedIngoingDimensionSpacePoint,
                            $sourceParentNode,
                            nodeAlias: 'peerParentNode'
                        )
                        ->returns('DISTINCT peerParentNode')
                        ->build()
                );
                try {
                    $peerParentNode = $peerParentNodeResult->getAsCypherMap(0)->getAsNode('peerParentNode');
                } catch (\OutOfBoundsException) {
                    // TODO: throw correctly!
                    throw new \Exception('wrong', 1750009438);
                }
                $peerSucceedingSiblingNodeAggregateId = $variantSucceedingSiblings
                    ->getSucceedingSiblingIdForDimensionSpacePoint($unassignedIngoingDimensionSpacePoint);

                $this->copyHierarchyRelation(
                    $sourceRelationship,
                    $peerNode,
                    $peerParentNode,
                    $unassignedIngoingDimensionSpacePoint,
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($peerParentNode->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $peerSucceedingSiblingNodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $unassignedIngoingDimensionSpacePoint
                    ),
                    copyDisabledState: false,
                );
            }
        }
    }

    private function whenNodePropertiesWereSet(NodePropertiesWereSet $event, EventEnvelope $eventEnvelope): void
    {
        $affectedNodeResult = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph(
                    $event->contentStreamId,
                    $event->originDimensionSpacePoint->toDimensionSpacePoint(),
                    $event->nodeAggregateId,
                )
                ->returns('n')
                ->build()
        );
        try {
            $affectedNode = $affectedNodeResult->getAsCypherMap(0)->getAsNode('n');
        } catch (\Exception) {
            throw new \InvalidArgumentException(
                'Cannot update node with copy on write since no anchor point could be resolved for node '
                . $event->getNodeAggregateId()->value . ' in content stream '
                . $event->getContentStreamId()->value,
                1750067053
            );
        }

        $affectedNode = $this->cloneNodeIfRequired($event->contentStreamId, $affectedNode, true);
        $this->client->runStatement(
            Statement::create(
                'MATCH (n) WHERE ID(n) = $nodeId
                    SET n.properties = $nodeTypeName
                    SET n.lastModified = $lastModified
                    SET n.originalLastModified = $originalLastModified',
                [
                    'nodeId' => $affectedNode->getId(),
                    'nodeTypeName' => $this->mergeNodeProperties(
                        $event->propertyValues,
                        $event->propertiesToUnset,
                        $event->getNodeAggregateId(),
                        $event->getContentStreamId(),
                        $event->getOriginDimensionSpacePoint()
                    ),
                    'lastModified' => $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM),
                    'originalLastModified' => self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                ]
            )
        );
    }

    private function cloneNodeIfRequired(
        ContentStreamId $contentStreamIdWhereWriteOccurs,
        Node $affectedNode,
    ): Node {
        // Get all content streams where the node is referenced
        $nodeContentStreams = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->match('(n:Node {aggregateId: $aggregateId})-[rel:IS_CHILD]->()')
                ->withParameter('aggregateId', $affectedNode->getProperty('aggregateId'))
                ->returns('rel as rels, COUNT(DISTINCT rel) as count')
                ->build()
        );

        if ($nodeContentStreams->getAsCypherMap(0)->getAsInt('count') > 1) {
            $affectedNode = $this->cloneNode($affectedNode, $contentStreamIdWhereWriteOccurs);
        }

        return $affectedNode;
    }

    private function whenNodeReferencesWereSet(NodeReferencesWereSet $event, EventEnvelope $eventEnvelope): void
    {
        foreach ($event->affectedSourceOriginDimensionSpacePoints as $dimensionSpacePoint) {
            $affectedNodeResult = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForSubgraph(
                        $event->contentStreamId,
                        $dimensionSpacePoint->toDimensionSpacePoint(),
                        $event->nodeAggregateId,
                    )
                    ->returns('n')
                    ->build()
            );
            try {
                $affectedNode = $affectedNodeResult->getAsCypherMap(0)->getAsNode('n');
            } catch (\Exception) {
                throw new \InvalidArgumentException(
                    'Cannot update node with copy on write since no anchor point could be resolved for node '
                    . $event->getNodeAggregateId()->value . ' in content stream '
                    . $event->getContentStreamId()->value,
                    1750067053
                );
            }

            $this->cloneNodeIfRequired($event->contentStreamId, $affectedNode);

            // WE NEED COPY ON WRITE HERE

            $this->clearReferenceRelations(
                $event->nodeAggregateId,
                $event->contentStreamId,
                $dimensionSpacePoint->toDimensionSpacePoint(),
                $eventEnvelope->recordedAt,
                self::initiatingDateTime($eventEnvelope),
                $event->references,
            );

            $this->createReferenceRelations(
                $event->nodeAggregateId,
                $event->contentStreamId,
                $dimensionSpacePoint->toDimensionSpacePoint(),
                $event->references,
                $eventEnvelope->recordedAt,
                self::initiatingDateTime($eventEnvelope),
            );
        }
    }

    private function whenNodeSpecializationVariantWasCreated(
        NodeSpecializationVariantWasCreated $event,
        EventEnvelope $eventEnvelope
    ): void {
        $nodeCloneResults = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph(
                    $event->contentStreamId,
                    $event->sourceOrigin->toDimensionSpacePoint(),
                    $event->nodeAggregateId,
                )
                ->call('apoc.refactor.cloneNodes([n], true)')
                ->yield('output AS generalizedNode')
                ->setProperty('originDimensionSpacePointHash', $event->specializationOrigin->toDimensionSpacePoint()->hash,
                    'generalizedNode')
                ->setProperty('created', $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM), 'generalizedNode')
                ->setProperty('originalCreated', self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    'generalizedNode')
                ->with('generalizedNode, n, p, rel')
                ->optionalMatch('(generalizedNode)-[generalizedRel:IS_CHILD]-() DELETE generalizedRel')
                ->returns('*')
                ->build()
        );
        if (empty($nodeCloneResults->getAsCypherMap(0)) || empty($nodeCloneResults->getAsCypherMap(0)->get('p'))) {
            throw new \RuntimeException(sprintf('Failed to create node generalization variant for node "%s" in sub graph %s@%s because the source parent node is missing',
                $event->nodeAggregateId->value, $event->sourceOrigin->toJson(), $event->contentStreamId->value), 1749910013);
        }
        if (empty($nodeCloneResults->getAsCypherMap(0)) || empty($nodeCloneResults->getAsCypherMap(0)->get('n'))) {
            throw new \RuntimeException(sprintf('Failed to create node generalization variant for node "%s" in sub graph %s@%s because the source node is missing',
                $event->nodeAggregateId->value, $event->sourceOrigin->toJson(), $event->contentStreamId->value), 1749910075);
        }

        $sourceRelationship = $nodeCloneResults->getAsCypherMap(0)->getAsRelationship('rel');
        $this->copyReferenceRelations(
            $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
            $nodeCloneResults->getAsCypherMap(0)->getAsNode('generalizedNode')
        );

        $sourceParentNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('p');
        $generalizedNode = $nodeCloneResults->getAsCypherMap(0)->getAsNode('generalizedNode');
        // Find all ingoing outgoing relationships (node is source of IS_CHILD) and change the source to the generalized node that are in the given dsp set
        $unassignedIngoingDimensionSpacePoints = [];
        $variantSucceedingSiblings = $event->specializationSiblings;
        foreach ($variantSucceedingSiblings as $sibling) {
            $existingChildRelationships = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForSubgraph(
                        $event->contentStreamId,
                        $sibling->dimensionSpacePoint,
                        $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
                    )
                    ->returns('n, rel')
                    ->build()
            );
            if ($existingChildRelationships->isEmpty()) {
                $unassignedIngoingDimensionSpacePoints[] = $sibling->dimensionSpacePoint;
            }
            foreach ($existingChildRelationships as $existingChildRelationship) {
                $this->moveChildHierarchyRelation(
                    $generalizedNode,
                    $existingChildRelationship->getAsRelationship('rel'),
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($nodeCloneResults->getAsCypherMap(0)->getAsNode('p')->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $sibling->dimensionSpacePoint,
                    ),
                );
            }
            $existingParentRelationships = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchChildrenForSubgraph(
                        $event->contentStreamId,
                        $sibling->dimensionSpacePoint,
                        $nodeCloneResults->getAsCypherMap(0)->getAsNode('n'),
                    )
                    ->returns('child, rel')
                    ->build()
            );
            foreach ($existingParentRelationships as $existingParentRelationship) {
                $this->moveParentHierarchyRelation(
                    $generalizedNode,
                    $existingParentRelationship->getAsRelationship('rel'),
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                        parentAggregateId: NodeAggregateId::fromString($nodeCloneResults->getAsCypherMap(0)->getAsNode('p')->getProperty('aggregateId')),
                        succeedingSiblingAggregateId: $sibling->nodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $sibling->dimensionSpacePoint,
                    ),
                );
            }
        }

        if (count($unassignedIngoingDimensionSpacePoints) > 0) {
            foreach ($unassignedIngoingDimensionSpacePoints as $unassignedIngoingDimensionSpacePoint) {
                $generalizationParentNodeResult = $this->client->runStatement(
                    NodeQueryBuilder::createForNodes()
                        ->matchNodeForSubgraph(
                            $event->contentStreamId,
                            $unassignedIngoingDimensionSpacePoint,
                            $sourceParentNode,
                            nodeAlias: 'generalizationParentNode'
                        )
                        ->returns('DISTINCT generalizationParentNode')
                        ->build()
                );
                try {
                    $generalizationParentNode = $generalizationParentNodeResult->getAsCypherMap(0)->getAsNode('generalizationParentNode');
                } catch (\OutOfBoundsException) {
                    // TODO: throw correctly!
                    throw new \Exception('wrong', 1750009438);
                }
                $generalizationSucceedingSiblingNodeAggregateId = $variantSucceedingSiblings
                    ->getSucceedingSiblingIdForDimensionSpacePoint($unassignedIngoingDimensionSpacePoint);

                $this->copyHierarchyRelation(
                    $sourceRelationship,
                    $generalizedNode,
                    $generalizationParentNode,
                    $unassignedIngoingDimensionSpacePoint,
                    $this->projectionContentGraph->determineHierarchyRelationPosition(
                    //parentAggregateId: $generalizationParentNode,
                        parentAggregateId: null,
                        succeedingSiblingAggregateId: $generalizationSucceedingSiblingNodeAggregateId,
                        contentStreamId: $event->contentStreamId,
                        dimensionSpacePoint: $unassignedIngoingDimensionSpacePoint
                    ),
                );
            }
        }
    }

    private function whenRootNodeAggregateDimensionsWereUpdated(RootNodeAggregateDimensionsWereUpdated $event): void
    {
        // TODO: Implement
    }

    private function whenRootNodeAggregateWithNodeWasCreated(
        RootNodeAggregateWithNodeWasCreated $event,
        EventEnvelope $eventEnvelope
    ): void {
        // Create the Root aggregate node

        $this->dimensionSpacePointsRepository->insertDimensionSpacePoint(OriginDimensionSpacePoint::createWithoutDimensions());

        // Create Node instances and IS_ROOT relationships for all covered dimension space points
        foreach ($event->coveredDimensionSpacePoints as $dimensionSpacePoint) {
            $createdNode = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->merge('(node:Node {
                        aggregateId: $aggregateId,
                        nodeTypeName: $nodeTypeName,
                        classification: $classification,
                        originDimensionSpacePointHash: $originDimensionSpacePointHash,
                        created: $created,
                        originalCreated: $originalCreated
                    })')
                    ->withParameters([
                        'aggregateId' => $event->nodeAggregateId->value,
                        'nodeTypeName' => $event->nodeTypeName->value,
                        'classification' => $event->nodeAggregateClassification->value,
                        'originDimensionSpacePointHash' => OriginDimensionSpacePoint::createWithoutDimensions()->hash,
                        'created' => $eventEnvelope->recordedAt->format(DateTimeInterface::ATOM),
                        'originalCreated' => self::initiatingDateTime($eventEnvelope)->format(DateTimeInterface::ATOM),
                    ])
                    ->returns('node')
                    ->build()
            );
            $newlyCreatedChildNode = $createdNode->getAsCypherMap(0)->getAsNode('node');
            $this->addRootRelation(
                $newlyCreatedChildNode,
                $event->contentStreamId,
                $dimensionSpacePoint,
                $eventEnvelope->recordedAt,
                self::initiatingDateTime($eventEnvelope)
            );
            $this->dimensionSpacePointsRepository->insertDimensionSpacePoint($dimensionSpacePoint);
        }
    }

    private function whenRootWorkspaceWasCreated(RootWorkspaceWasCreated $event): void
    {
        $this->createWorkspace($event->workspaceName, null, $event->newContentStreamId);
    }

    private function WhenSubtreeWasTagged(SubtreeWasTagged $event): void
    {
        $this->addSubtreeTag($event->contentStreamId, $event->nodeAggregateId, $event->affectedDimensionSpacePoints, $event->tag);
    }

    private function whenSubtreeWasUntagged(SubtreeWasUntagged $event): void
    {
        $this->removeSubtreeTag($event->contentStreamId, $event->nodeAggregateId, $event->affectedDimensionSpacePoints,
            $event->tag);
    }

    private function whenWorkspaceBaseWorkspaceWasChanged(WorkspaceBaseWorkspaceWasChanged $event): void
    {
        $this->updateBaseWorkspace($event->workspaceName, $event->baseWorkspaceName, $event->newContentStreamId);
    }

    private function whenWorkspaceRebaseFailed(WorkspaceRebaseFailed $event): void
    {
        $this->reopenContentStream($event->sourceContentStreamId);
    }

    private function whenWorkspaceWasCreated(WorkspaceWasCreated $event): void
    {
        $this->createWorkspace($event->workspaceName, $event->baseWorkspaceName, $event->newContentStreamId);
    }

    private function whenWorkspaceWasDiscarded(WorkspaceWasDiscarded $event): void
    {
        $this->updateWorkspaceContentStreamId($event->workspaceName, $event->newContentStreamId);
    }

    private function whenWorkspaceWasPublished(WorkspaceWasPublished $event): void
    {
        $this->updateWorkspaceContentStreamId($event->sourceWorkspaceName, $event->newSourceContentStreamId);
    }

    private function whenWorkspaceWasRebased(WorkspaceWasRebased $event): void
    {
        $this->updateWorkspaceContentStreamId($event->workspaceName, $event->newContentStreamId);
    }

    private function whenWorkspaceWasRemoved(WorkspaceWasRemoved $event): void
    {
        $this->removeWorkspace($event->workspaceName);
    }

    private function mergeNodeProperties(
        $propertyValues,
        $propertiesToUnset,
        $nodeAggregateId,
        $contentStreamId,
        $originDimensionSpacePoint
    ): string {
        // First get existing properties
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (node:Node {aggregateId: $aggregateId})
                WHERE EXISTS {
                    MATCH (node)-[:IS_CHILD {
                        contentStreamId: $contentStreamId,
                        dimensionSpacePointHash: $originDimensionSpacePointHash
                    }]->()
                } OR EXISTS {
                    MATCH (node)-[:IS_ROOT {
                        contentStreamId: $contentStreamId,
                        dimensionSpacePointHash: $originDimensionSpacePointHash
                    }]->()
                }
                RETURN node.properties AS properties',
                [
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $contentStreamId->value,
                    'originDimensionSpacePointHash' => $originDimensionSpacePoint->hash,
                ]
            )
        );

        // Start with existing SerializedPropertyValues structure or create empty one
        $existingSerializedProperties = [];
        if ($result->count() > 0) {
            $record = $result->first();
            $propertiesJson = $record->get('properties');
            if ($propertiesJson && $propertiesJson !== '{}') {
                $existingSerializedProperties = json_decode($propertiesJson, true) ?? [];
            }
        }

        // Merge with new property values, preserving SerializedPropertyValue format
        foreach ($propertyValues->values as $propertyName => $serializedPropertyValue) {
            $existingSerializedProperties[$propertyName] = $serializedPropertyValue->jsonSerialize();
        }

        // Remove properties that should be unset
        foreach ($propertiesToUnset as $propertyName) {
            unset($existingSerializedProperties[$propertyName->value]);
        }

        return json_encode($existingSerializedProperties);
    }

    private function checkResultKey(SummarizedResult $result, string $nodeKey): bool
    {
        return $result->hasKey(0) && $result->getAsCypherMap(0)->hasKey($nodeKey) && !empty($result->getAsCypherMap(0)->get($nodeKey));
    }

    public function resetState(): void
    {
        $this->client->runStatement(Statement::create('MATCH (n) DETACH DELETE n'));
    }

    public function status(): ProjectionStatus
    {
        if (!$this->client->verifyConnectivity()) {
            return ProjectionStatus::error('Failed to connect to database');
        }

        $currentIndexes = $this->client->run('SHOW INDEXES');
        if (empty(array_udiff_assoc($currentIndexes->toArray(), $this->requiredIndexes, fn(CypherMap $a, array $b) => strcmp($a['name'], $b['name'])))) {
            return ProjectionStatus::ok();
        }
        return ProjectionStatus::setupRequired('setup required');
    }

    private static function initiatingDateTime(EventEnvelope $eventEnvelope): DateTimeImmutable
    {
        if ($eventEnvelope->event->metadata?->has(InitiatingEventMetadata::INITIATING_TIMESTAMP) !== true) {
            return $eventEnvelope->recordedAt;
        }
        $initiatingTimestamp = InitiatingEventMetadata::getInitiatingTimestamp($eventEnvelope->event->metadata);
        if ($initiatingTimestamp === null) {
            throw new \RuntimeException(sprintf('Failed to extract initiating timestamp from event "%s"',
                $eventEnvelope->event->id->value), 1678902291);
        }
        return $initiatingTimestamp;
    }
}
