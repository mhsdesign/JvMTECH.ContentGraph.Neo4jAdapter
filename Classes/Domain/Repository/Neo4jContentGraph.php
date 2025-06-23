<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository;

use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\NodeQueryBuilder;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Types\CypherMap;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePointSet;
use Neos\ContentRepository\Core\DimensionSpace\OriginDimensionSpacePoint;
use Neos\ContentRepository\Core\Feature\SubtreeTagging\Dto\SubtreeTag;
use Neos\ContentRepository\Core\NodeType\NodeTypeManager;
use Neos\ContentRepository\Core\NodeType\NodeTypeName;
use Neos\ContentRepository\Core\NodeType\NodeTypeNames;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphInterface;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentSubgraphInterface;
use Neos\ContentRepository\Core\Projection\ContentGraph\Filter;
use Neos\ContentRepository\Core\Projection\ContentGraph\NodeAggregate;
use Neos\ContentRepository\Core\Projection\ContentGraph\NodeAggregates;
use Neos\ContentRepository\Core\Projection\ContentGraph\VisibilityConstraints;
use Neos\ContentRepository\Core\SharedModel\ContentRepository\ContentRepositoryId;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateClassification;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateIds;
use Neos\ContentRepository\Core\SharedModel\Node\NodeName;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\ContentRepository\Core\SharedModel\Workspace\WorkspaceName;

class Neo4jContentGraph implements ContentGraphInterface
{
    public function __construct(
        private readonly ContentRepositoryId $contentRepositoryId,
        private readonly WorkspaceName $workspaceName,
        private readonly ClientInterface $client,
        private readonly ContentStreamId $contentStreamId,
        private readonly NodeFactory $nodeFactory,
        private readonly Neo4jDimensionSpacePointsRepository $dimensionSpacePointsRepository,
        private readonly NodeTypeManager $nodeTypeManager,
    ) {
    }

    /**
     * @inheritDoc
     */
    public function getContentRepositoryId(): ContentRepositoryId
    {
        return $this->contentRepositoryId;
    }

    /**
     * @inheritDoc
     */
    public function getWorkspaceName(): WorkspaceName
    {
        return $this->workspaceName;
    }

    /**
     * @inheritDoc
     */
    public function getSubgraph(
        DimensionSpacePoint $dimensionSpacePoint,
        VisibilityConstraints $visibilityConstraints
    ): ContentSubgraphInterface {
        return new Neo4jContentSubgraph(
            $this->contentRepositoryId,
            $this->workspaceName,
            $dimensionSpacePoint,
            $visibilityConstraints,
            $this->getContentStreamId(),
            $this->client,
            $this->nodeFactory,
            $this->nodeTypeManager,
        );
    }

    /**
     * @inheritDoc
     */
    public function findRootNodeAggregateByType(NodeTypeName $nodeTypeName): ?NodeAggregate
    {
        $filter = Filter\FindRootNodeAggregatesFilter::create($nodeTypeName);
        $aggregates = $this->findRootNodeAggregates($filter);
        if ($aggregates->isEmpty()) {
            return null;
        }
        return $aggregates->first();
    }

    /**
     * @inheritDoc
     */
    public function findRootNodeAggregates(Filter\FindRootNodeAggregatesFilter $filter): NodeAggregates
    {
        $statement = NodeQueryBuilder::createForNodes()
            ->matchNodeWithRootRelation($this->getContentStreamId())
            ->where(!empty($filter->nodeTypeName) ? 'n.nodeTypeName = $nodeTypeName' : '')
            ->withParameter('nodeTypeName', $filter->nodeTypeName?->value ?: '')
            ->returnStandardNodeFields()
            ->build();
        $result = $this->client->runStatement($statement);

        if ($result->isEmpty()) {
            return NodeAggregates::createEmpty();
        }
        return $this->nodeFactory->mapResultToNodeAggregates(
            $result,
            $this->workspaceName,
        );
    }

    /**
     * @inheritDoc
     */
    public function findNodeAggregatesByType(NodeTypeName $nodeTypeName): NodeAggregates
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->match('(n:Node {nodeTypeName: $nodeTypeName})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()')
                ->withParameters([
                    'nodeTypeName' => $nodeTypeName->value,
                    'contentStreamId' => $this->contentStreamId->value,
                ])
                ->returnStandardNodeFields()
                ->build()
        );

        return $this->nodeFactory->mapResultToNodeAggregates(
            $result,
            $this->workspaceName,
        );
    }

    /**
     * @inheritDoc
     */
    public function findNodeAggregateById(NodeAggregateId $nodeAggregateId): ?NodeAggregate
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->match('(n:Node {aggregateId: $aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()')
                ->withParameters([
                    'aggregateId' => $nodeAggregateId->value,
                    'contentStreamId' => $this->getContentStreamId()->value,
                ])
                ->returnStandardNodeFields()
                ->build()
        );
        return $this->nodeFactory->mapResultToNodeAggregate(
            $result,
            $this->workspaceName,
        );
    }

    /**
     * @inheritDoc
     */
    public function findNodeAggregatesByIds(NodeAggregateIds $nodeAggregateIds): NodeAggregates
    {
        $nodeAggregates = [];
        foreach ($nodeAggregateIds as $nodeAggregateId) {
            $result = $this->client->runStatement(
                NodeQueryBuilder::createForNodes()
                    ->matchNodeForContentStream($this->contentStreamId, $nodeAggregateId)
                    ->returnStandardNodeFields()
                    ->build()
            );
            $nodeAggregates[] = $this->nodeFactory->mapResultToNodeAggregate(
                $result,
                $this->workspaceName,
            );
        }

        return NodeAggregates::fromArray($nodeAggregates);
    }

    public function findUsedNodeTypeNames(): NodeTypeNames
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (n:Node) RETURN DISTINCT n.nodeTypeName AS nodeTypeName',
                []
            )
        );

        $nodeTypeNames = [];
        foreach ($result as $record) {
            $nodeTypeNames[] = NodeTypeName::fromString($record->get('nodeTypeName'));
        }

        return NodeTypeNames::fromArray($nodeTypeNames);
    }

    public function findParentNodeAggregateByChildOriginDimensionSpacePoint(
        NodeAggregateId $childNodeAggregateId,
        OriginDimensionSpacePoint $childOriginDimensionSpacePoint
    ): ?NodeAggregate {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph($this->contentStreamId, $childOriginDimensionSpacePoint->toDimensionSpacePoint(), $childNodeAggregateId, '', '', 'parentNode')
                ->match('(n:Node {aggregateId: parentNode.aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()')
                ->returnStandardNodeFields()
                ->build()
        );

        if ($result->isEmpty()) {
            return null;
        }
        return $this->nodeFactory->mapResultToNodeAggregate(
            $result,
            $this->workspaceName,
        );
    }

    /**
     * @inheritDoc
     */
    public function findParentNodeAggregates(NodeAggregateId $childNodeAggregateId): NodeAggregates
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->match('(:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId}]->(parentNode:Node)')
                ->match('(n:Node {aggregateId: parentNode.aggregateId})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->()')
                ->withParameters([
                    'aggregateId' => $childNodeAggregateId->value,
                    'contentStreamId' => $this->getContentStreamId()->value,
                ])
                ->returnStandardNodeFields()
                ->build()
        );

        if ($result->isEmpty()) {
            return NodeAggregates::createEmpty();
        }
        return $this->nodeFactory->mapResultToNodeAggregates(
            $result,
            $this->workspaceName,
        );
    }

    /**
     * @inheritDoc
     */
    public function findAncestorNodeAggregateIds(NodeAggregateId $entryNodeAggregateId): NodeAggregateIds
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (n:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId}]->(ancestorNode:Node)
                RETURN DISTINCT ancestorNode.aggregateId AS aggregateId',
                [
                    'aggregateId' => $entryNodeAggregateId->value,
                    'contentStreamId' => $this->getContentStreamId()->value,
                ]
            )
        );
        if ($result->isEmpty()) {
            return NodeAggregateIds::createEmpty();
        }
        return NodeAggregateIds::fromArray($result->map(fn(CypherMap $row) => ($row->hasKey('aggregateId') ? NodeAggregateId::fromString($row->get('aggregateId')) : null))->toArray());
    }

    /**
     * @inheritDoc
     */
    public function findChildNodeAggregates(NodeAggregateId $parentNodeAggregateId): NodeAggregates
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForContentStream($this->contentStreamId, $parentNodeAggregateId, 'original', '')
                ->match('(n:Node)-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->(original)')
                ->returnStandardNodeFields()
                ->build()
        );

        if ($result->isEmpty()) {
            return NodeAggregates::createEmpty();
        }

        return $this->nodeFactory->mapResultToNodeAggregates($result, $this->workspaceName);
    }

    /**
     * @inheritDoc
     */
    public function findChildNodeAggregateByName(NodeAggregateId $parentNodeAggregateId, NodeName $name): ?NodeAggregate
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->match('(original:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId}]->()')
                ->match('(n:Node {name: $name})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->(original)')
                ->withParameters(
                    [
                        'aggregateId' => $parentNodeAggregateId->value,
                        'contentStreamId' => $this->contentStreamId->value,
                        'name' => $name->value,
                    ]
                )
                ->returnStandardNodeFields()
                ->build()
        );

        if ($result->isEmpty()) {
            return null;
        }

        return $this->nodeFactory->mapResultToNodeAggregate($result, $this->workspaceName);
    }

    /**
     * @inheritDoc
     */
    public function findTetheredChildNodeAggregates(NodeAggregateId $parentNodeAggregateId): NodeAggregates
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForContentStream($this->contentStreamId, $parentNodeAggregateId, 'original', '')
                ->match('(n:Node {classification: $classification})-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->(original)')
                ->withParameter('classification', NodeAggregateClassification::CLASSIFICATION_TETHERED->value)
                ->returnStandardNodeFields()
                ->build()
        );

        if ($result->isEmpty()) {
            return NodeAggregates::createEmpty();
        }

        return $this->nodeFactory->mapResultToNodeAggregates($result, $this->workspaceName);
    }

    /**
     * @inheritDoc
     */
    public function getDimensionSpacePointsOccupiedByChildNodeName(
        NodeName $nodeName,
        NodeAggregateId $parentNodeAggregateId,
        OriginDimensionSpacePoint $parentNodeOriginDimensionSpacePoint,
        DimensionSpacePointSet $dimensionSpacePointsToCheck
    ): DimensionSpacePointSet {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (parentNode:Node {aggregateId: $aggregateId, originDimensionSpacePointHash: $originDimensionSpacePointHash})-[:IS_CHILD {contentStreamId: $contentStreamId}]->()
                    MATCH (parentNode)<-[rel:IS_CHILD {contentStreamId: $contentStreamId}]->(:Node {name: $nodeName})
                    RETURN rel',
                [
                    'aggregateId' => $parentNodeAggregateId->value,
                    'originDimensionSpacePointHash' => $parentNodeOriginDimensionSpacePoint->hash,
                    'contentStreamId' => $this->contentStreamId->value,
                    'nodeName' => $nodeName->value,
                ]
            )
        );

        $dimensionSpacePoints = [];
        foreach ($result as $item) {
            if (!$item instanceof CypherMap) {
                throw new \Exception('what the fuck, no cyphermap', 1749647475);
            }

            $dimensionSpacePointHash = $item->getAsRelationship('rel')->getProperty('dimensionSpacePointHash');
            $originDimensionSpacePoint = $this->dimensionSpacePointsRepository->getOriginDimensionSpacePointByHash($dimensionSpacePointHash);
            $dimensionSpacePoints[$dimensionSpacePointHash] = $originDimensionSpacePoint->toDimensionSpacePoint();
        }
        return DimensionSpacePointSet::fromArray($dimensionSpacePoints);
    }

    /**
     * @inheritDoc
     */
    public function findNodeAggregatesTaggedBy(SubtreeTag $subtreeTag): NodeAggregates
    {
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodesForContentStream($this->contentStreamId)
                ->where(sprintf('apoc.convert.fromJsonMap(rel.subtreeTags).%s', $subtreeTag->value))
                ->returnStandardNodeFields()
                ->build()
        );

        if ($result->isEmpty()) {
            return NodeAggregates::createEmpty();
        }

        return $this->nodeFactory->mapResultToNodeAggregates($result, $this->workspaceName);
    }

    /**
     * @inheritDoc
     */
    public function getContentStreamId(): ContentStreamId
    {
        return $this->contentStreamId;
    }
}
