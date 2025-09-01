<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository;

use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\NodeQueryBuilder;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query\QueryBuilder;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\Feature\SubtreeTagging\Dto\SubtreeTags;
use Neos\ContentRepository\Core\NodeType\NodeTypeManager;
use Neos\ContentRepository\Core\NodeType\NodeTypeName;
use Neos\ContentRepository\Core\Projection\ContentGraph\AbsoluteNodePath;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentSubgraphInterface;
use Neos\ContentRepository\Core\Projection\ContentGraph\Filter;
use Neos\ContentRepository\Core\Projection\ContentGraph\Filter\NodeType\ExpandedNodeTypeCriteria;
use Neos\ContentRepository\Core\Projection\ContentGraph\Node;
use Neos\ContentRepository\Core\Projection\ContentGraph\NodePath;
use Neos\ContentRepository\Core\Projection\ContentGraph\Nodes;
use Neos\ContentRepository\Core\Projection\ContentGraph\References;
use Neos\ContentRepository\Core\Projection\ContentGraph\Subtree;
use Neos\ContentRepository\Core\Projection\ContentGraph\Subtrees;
use Neos\ContentRepository\Core\Projection\ContentGraph\VisibilityConstraints;
use Neos\ContentRepository\Core\SharedModel\ContentRepository\ContentRepositoryId;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateIds;
use Neos\ContentRepository\Core\SharedModel\Node\NodeName;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\ContentRepository\Core\SharedModel\Workspace\WorkspaceName;

class Neo4jContentSubgraph implements ContentSubgraphInterface
{
    public function __construct(
        private readonly ContentRepositoryId $contentRepositoryId,
        private readonly WorkspaceName $workspaceName,
        private readonly DimensionSpacePoint $dimensionSpacePoint,
        private readonly VisibilityConstraints $visibilityConstraints,
        private readonly ContentStreamId $contentStreamId,
        private readonly ClientInterface $client,
        private readonly NodeFactory $nodeFactory,
        private readonly NodeTypeManager $nodeTypeManager,
        private readonly bool $debug = false,
    )
    {
    }

    public function getContentRepositoryId(): ContentRepositoryId
    {
        if ($this->debug) \Neos\Flow\var_dump('getContentRepositoryId called');
        return $this->contentRepositoryId;
    }

    public function getWorkspaceName(): WorkspaceName
    {
        if ($this->debug) \Neos\Flow\var_dump('getWorkspaceName called');
        return $this->workspaceName;
    }

    public function getDimensionSpacePoint(): DimensionSpacePoint
    {
        if ($this->debug) \Neos\Flow\var_dump('getDimensionSpacePoint called');
        return $this->dimensionSpacePoint;
    }

    public function getVisibilityConstraints(): VisibilityConstraints
    {
        if ($this->debug) \Neos\Flow\var_dump('getVisibilityConstraints called');
        return $this->visibilityConstraints;
    }

    public function findNodeById(NodeAggregateId $nodeAggregateId): ?Node
    {
        if ($this->debug) \Neos\Flow\var_dump('findNodeById called with' . $nodeAggregateId->value);
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph(
                    $this->contentStreamId,
                    $this->dimensionSpacePoint,
                    $nodeAggregateId,
                )
                ->withVisibilityConstraints($this->visibilityConstraints)
                ->returns('n, rel')
                ->build()
        );

        if ($result->isEmpty() || !$result->hasKey(0) || !$result->getAsCypherMap(0)->hasKey('n')) {
            return null;
        }

        return $this->nodeFactory->mapResultToNode(
            $result->getAsCypherMap(0),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function findNodesByIds(NodeAggregateIds $nodeAggregateIds): Nodes
    {
        if ($this->debug) \Neos\Flow\var_dump('findNodesByIds called with' . implode(',', $nodeAggregateIds->map(fn(NodeAggregateId $id) => $id->value)));
        $nodes = [];
        foreach ($nodeAggregateIds as $nodeAggregateId) {
            $node = $this->findNodeById($nodeAggregateId);
            if ($node !== null) {
                $nodes[] = $node;
            }
        }
        return Nodes::fromArray($nodes);
    }

    public function findRootNodeByType(NodeTypeName $nodeTypeName): ?Node
    {
        if ($this->debug) \Neos\Flow\var_dump('findRootNodeByType called with' . $nodeTypeName->value);
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (n:Node {nodeTypeName: $nodeTypeName})-[:IS_CHILD{contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(:Root) RETURN DISTINCT n',
                [
                    'nodeTypeName' => $nodeTypeName->value,
                    'dimensionSpacePointHash' => $this->dimensionSpacePoint->hash,
                    'contentStreamId' => $this->contentStreamId->value,
                ]
            ),
        );

        if ($result->isEmpty() || !$result->hasKey(0) || !$result->getAsCypherMap(0) || !$result->getAsCypherMap(0)->hasKey('n')) {
            return null;
        }

        return $this->nodeFactory->mapResultToNode(
            $result->getAsCypherMap(0)->getAsNode('n'),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function findChildNodes(NodeAggregateId $parentNodeAggregateId, Filter\FindChildNodesFilter $filter): Nodes
    {
        if ($this->debug) \Neos\Flow\var_dump('findChildNodes called with parentNodeAggregateId: ' . $parentNodeAggregateId->value);
        $query = $this->getChildNodesQuery(
            $parentNodeAggregateId,
            $filter
        );
        $query->returns('DISTINCT child');

        $result = $this->client->runStatement($query->build());
        if ($result->isEmpty()) {
            return Nodes::fromArray([]);
        }

        return $this->nodeFactory->mapResultToNodes(
            array_map(fn(CypherMap $row) => ($row->getAsNode('child')), $result->toArray()),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function countChildNodes(NodeAggregateId $parentNodeAggregateId, Filter\CountChildNodesFilter $filter): int
    {
        if ($this->debug) \Neos\Flow\var_dump('countChildNodes called with parentNodeAggregateId: ' . $parentNodeAggregateId->value);
        $query = $this->getChildNodesQuery(
            $parentNodeAggregateId,
            $filter
        );
        $query->returns('count(DISTINCT child) as count');
        return $this->client->runStatement($query->build())->getAsCypherMap(0)->getAsInt('count');
    }

    private function getChildNodesQuery(
        NodeAggregateId $parentNodeAggregateId,
        Filter\FindChildNodesFilter|Filter\CountChildNodesFilter $filter,
    ): NodeQueryBuilder
    {
        $query = NodeQueryBuilder::createForNodes()
            ->matchChildrenForSubgraph(
                $this->contentStreamId,
                $this->dimensionSpacePoint,
                $parentNodeAggregateId,
                parentAlias: '',
            );
        /** @var SubtreeTags $constraint */
        foreach ($this->visibilityConstraints as $constraint) {
            foreach ($constraint as $subtreeTag) {
                $query->with(sprintf('child, rel, COALESCE(apoc.convert.fromJsonMap(rel.subtreeTags).%s, false) AS %s', $subtreeTag->value, $subtreeTag->value));
                $query->where(sprintf('%s <> true', $subtreeTag->value));
            }
        }

        if (!empty($filter->nodeTypes)) {
            $expandedNodeTypeCriteria = ExpandedNodeTypeCriteria::create($filter->nodeTypes, $this->nodeTypeManager);

            if (!$expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames, 'child');
            }
            if (!$expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames, 'child', 'disallowedNodeTypeNames', negate: true);
            }
        }

        if (!empty($filter->searchTerm)) {
            $query
                ->where('(child.name CONTAINS $searchTerm OR any(prop in keys(child.properties) WHERE child.properties[prop] CONTAINS $searchTerm))')
                ->withParameter('searchTerm', $filter->searchTerm->term);
        }

        if (!empty($filter->propertyValue)) {
            $propertyParams = [];
            $propertyCondition = $this->buildPropertyValueCondition($filter->propertyValue, 'child', $propertyParams);
            if ($propertyCondition !== null) {
                $query
                    ->where($propertyCondition)
                    ->withParameters($propertyParams);
            }
        }
        if ($filter->ordering !== null) {
            foreach ($filter->ordering as $ordering)
            $query->orderBy('rel.'.$ordering->field->value, $ordering->direction->value);
        }
        $query
            ->orderBy('rel.position')
            ->orderBy('child.aggregateId');
        if ($filter->pagination !== null) {
            $query
                ->limit($filter->pagination->limit)
                ->skip($filter->pagination->offset);
        }
        return $query;
    }

    public function findParentNode(NodeAggregateId $childNodeAggregateId): ?Node
    {
        if ($this->debug) \Neos\Flow\var_dump('findParentNode called with childNodeAggregateId: ' . $childNodeAggregateId->value);
        $result = $this->client->runStatement(
            NodeQueryBuilder::createForNodes()
                ->matchNodeForSubgraph(
                    $this->contentStreamId,
                    $this->dimensionSpacePoint,
                    $childNodeAggregateId,
                )
            ->where('"Node" IN labels(p)')
            ->returns('p as parent')
            ->build()
        );

        if ($result->isEmpty() || !$result->hasKey(0) || !$result->getAsCypherMap(0)->hasKey('parent')) {
            return null;
        }

        return $this->nodeFactory->mapResultToNode(
            $result->getAsCypherMap(0)->getAsNode('parent'),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function findSucceedingSiblingNodes(
        NodeAggregateId $siblingNodeAggregateId,
        Filter\FindSucceedingSiblingNodesFilter $filter
    ): Nodes {
        if ($this->debug) \Neos\Flow\var_dump('findSucceedingSiblingNodes called with siblingNodeAggregateId: ' . $siblingNodeAggregateId->value);
        $result = $this->client->runStatement(
            $this->getSiblingNodesQuery(
                $siblingNodeAggregateId,
                $filter,
            )->build(),
        );
        if ($result->isEmpty()) {
            return Nodes::fromArray([]);
        }
        return Nodes::fromArray(
            array_filter(
                array_map(
                    fn(CypherMap $map) => (
                        $map->hasKey('otherSibling') ? $this->nodeFactory->mapResultToNode(
                            $map->getAsNode('otherSibling'),
                            $this->workspaceName,
                            $this->dimensionSpacePoint,
                            $this->visibilityConstraints,
                        ) : null
                    ),
                    $result->toArray()
                )
            )
        );
    }

    public function findPrecedingSiblingNodes(
        NodeAggregateId $siblingNodeAggregateId,
        Filter\FindPrecedingSiblingNodesFilter $filter
    ): Nodes {
        if ($this->debug) \Neos\Flow\var_dump('findPrecedingSiblingNodes called with siblingNodeAggregateId: ' . $siblingNodeAggregateId->value);
        $result = $this->client->runStatement(
            $this->getSiblingNodesQuery(
                $siblingNodeAggregateId,
                $filter,
            )->build(),
        );
        if ($result->isEmpty()) {
            return Nodes::fromArray([]);
        }
        return Nodes::fromArray(
            array_filter(
                array_map(
                    fn(CypherMap $map) => (
                    $map->hasKey('otherSibling') ? $this->nodeFactory->mapResultToNode(
                        $map->getAsNode('otherSibling'),
                        $this->workspaceName,
                        $this->dimensionSpacePoint,
                        $this->visibilityConstraints,
                    ) : null
                    ),
                    $result->toArray()
                )
            )
        );
    }

    private function getSiblingNodesQuery(
        NodeAggregateId $siblingNodeAggregateId,
        Filter\FindSucceedingSiblingNodesFilter|Filter\FindPrecedingSiblingNodesFilter $filter
    ): NodeQueryBuilder {
        $query = NodeQueryBuilder::createForNodes()
            ->matchNodeForSubgraph(
                $this->contentStreamId,
                $this->dimensionSpacePoint,
                $siblingNodeAggregateId,
            )
            ->match('(p)<-[otherSiblingRel:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]-(otherSibling:Node)');

        foreach ($this->visibilityConstraints as $constraint) {
            foreach ($constraint as $subtreeTag) {
                $query->where(sprintf('COALESCE(apoc.convert.fromJsonMap(otherSiblingRel.subtreeTags).%s, false) <> true', $subtreeTag->value));
            }
        }

        if ($filter instanceof Filter\FindSucceedingSiblingNodesFilter) {
            $query
                ->where('otherSiblingRel.position > rel.position')
                ->orderBy('otherSiblingRel.position');
        } else {
            $query
                ->where('otherSiblingRel.position < rel.position')
                ->orderBy('otherSiblingRel.position', 'DESC');
        }

        if (!empty($filter->nodeTypes)) {
            $expandedNodeTypeCriteria = ExpandedNodeTypeCriteria::create($filter->nodeTypes, $this->nodeTypeManager);

            if (!$expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames, 'otherSibling');
            }
            if (!$expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames, 'otherSibling', 'disallowedNodeTypeNames', negate: true);
            }
        }

        if (!empty($filter->searchTerm)) {
            $query
                ->where('(otherSibling.name CONTAINS $searchTerm OR any(prop in keys(otherSibling.properties) WHERE otherSibling.properties[prop] CONTAINS $searchTerm))')
                ->withParameter('searchTerm', $filter->searchTerm->term);
        }

        if (!empty($filter->propertyValue)) {
            $propertyParams = [];
            $propertyCondition = $this->buildPropertyValueCondition($filter->propertyValue, 'otherSibling', $propertyParams);
            if ($propertyCondition !== null) {
                $query
                    ->where($propertyCondition)
                    ->withParameters($propertyParams);
            }
        }
        if ($filter->ordering !== null) {
            foreach ($filter->ordering as $ordering) {
                $query->orderBy('ref.' . $ordering->field->value, $ordering->direction->value);
            }
        }
        $query
            ->orderBy('otherSiblingRel.position')
            ->orderBy('otherSibling.aggregateid');
        if ($filter->pagination !== null) {
            $query
                ->limit($filter->pagination->limit)
                ->skip($filter->pagination->offset);
        }
        $query->returns('DISTINCT otherSibling');
        return $query;
    }

    public function findAncestorNodes(NodeAggregateId $entryNodeAggregateId, Filter\FindAncestorNodesFilter $filter): Nodes
    {
        if ($this->debug) \Neos\Flow\var_dump('findAncestorNodes called with entryNodeAggregateId: ' . $entryNodeAggregateId->value);
        $query = NodeQueryBuilder::createForNodes()
            ->matchRootPath(
                $entryNodeAggregateId,
                $this->contentStreamId,
                $this->dimensionSpacePoint,
            );
        $query->returns('nodes(path) as nodes');
        $result = $this->client->runStatement($query->build());
        if ($result->isEmpty() || !$result->hasKey(0)) {
            return Nodes::fromArray([]);
        }

        $nodes = $result->getAsCypherMap(0)->getAsCypherList('nodes');

        // TODO: Why filter later? What is this function supposed to do???
        $nodes = array_filter($nodes->toArray(), function (\Laudis\Neo4j\Types\Node $node) use ($filter) {
            if (!$node->getProperties()->offsetExists('nodeTypeName')) {
                return false;
            }
            $nodeTypeName = $node->getProperty('nodeTypeName');
            return is_string($nodeTypeName)
                && ($filter->nodeTypes === null || $filter->nodeTypes->explicitlyAllowedNodeTypeNames->isEmpty() || count(array_filter($filter->nodeTypes->explicitlyAllowedNodeTypeNames->map(fn(
                    $explicitlyAllowedNodeTypeName
                ) => $explicitlyAllowedNodeTypeName->value === $nodeTypeName))) > 0)
                && ($filter->nodeTypes === null || $filter->nodeTypes->explicitlyDisallowedNodeTypeNames->isEmpty() || count(array_filter($filter->nodeTypes->explicitlyDisallowedNodeTypeNames->map(fn(
                    $explicitlyDisallowedNodeTypeName
                ) => $explicitlyDisallowedNodeTypeName->value === $nodeTypeName))) === 0);
        });

        return $this->nodeFactory->mapResultToNodes(
            $nodes,
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function countAncestorNodes(NodeAggregateId $entryNodeAggregateId, Filter\CountAncestorNodesFilter $filter): int
    {
        if ($this->debug) \Neos\Flow\var_dump('countAncestorNodes called with entryNodeAggregateId: ' . $entryNodeAggregateId->value);
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (n:Node {aggregateId: $aggregateId})-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->()
                MATCH p = SHORTEST 1 (n)-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]-+(:Root) RETURN length(p) as length',
                [
                    'aggregateId' => $entryNodeAggregateId->value,
                    'contentStreamId' => $this->contentStreamId->value,
                    'dimensionSpacePointHash' => $this->dimensionSpacePoint->hash,
                ]
            ),
        );
        if ($result->isEmpty() || !$result->hasKey(0)) {
            return 0;
        }

        return $result->getAsCypherMap(0)->getAsInt('length') ;
    }

    public function findClosestNode(NodeAggregateId $entryNodeAggregateId, Filter\FindClosestNodeFilter $filter): ?Node
    {
        if ($this->debug) \Neos\Flow\var_dump('findClosestNode called with entryNodeAggregateId: ' . $entryNodeAggregateId->value);
        $query = NodeQueryBuilder::createForNodes()
            ->matchNodeForSubgraph(
                $this->contentStreamId,
                $this->dimensionSpacePoint,
                $entryNodeAggregateId,
                relationAlias: '',
                parentAlias: '',
            );
        // If there is no nodetype filter, we can directly return the node
        if (empty($filter->nodeTypes)) {
            return $this->nodeFactory->mapResultToNode(
                $this->client->runStatement(
                    $query->build()
                )->getAsCypherMap(0)->getAsNode('n'),
                $this->workspaceName,
                $this->dimensionSpacePoint,
                $this->visibilityConstraints,
            );
        }
        $query->match('p = (n)-[rel:IS_CHILD*1..]->(target:Root|Node) WHERE all(r in rel WHERE r.contentStreamId = $contentStreamId AND r.dimensionSpacePointHash = $dimensionSpacePointHash) RETURN nodes(p) as nodes');

        $nodeTypeCriteria = ExpandedNodeTypeCriteria::create(
            $filter->nodeTypes,
            $this->nodeTypeManager,
        );
        $result = $this->client->runStatement($query->build());
        if ($result->isEmpty() || !$result->hasKey(0) || !$result->getAsCypherMap(0)->hasKey('nodes')) {
            return null;
        }

        foreach ($result->reversed()->current()->getAsCypherMap('nodes') as $resultNode) {
            $nodeTypeName = NodeTypeName::fromString($resultNode->getProperty('nodeTypeName'));
            if ($nodeTypeCriteria->matches($nodeTypeName)) {
                return $this->nodeFactory->mapResultToNode(
                    $resultNode,
                    $this->workspaceName,
                    $this->dimensionSpacePoint,
                    $this->visibilityConstraints,
                );
            }
        }
        return null;
    }

    public function findDescendantNodes(NodeAggregateId $entryNodeAggregateId, Filter\FindDescendantNodesFilter $filter): Nodes
    {
        if ($this->debug) \Neos\Flow\var_dump('findDescendantNodes called with entryNodeAggregateId: ' . $entryNodeAggregateId->value);
        $query = NodeQueryBuilder::createForNodes()
            ->matchNodeForSubgraph(
                $this->contentStreamId,
                $this->dimensionSpacePoint,
                $entryNodeAggregateId,
            )
            ->match('path = (n)<-[descendantRel:IS_CHILD*1..]-(descendant:Node)')
            ->where('all(r in descendantRel WHERE r.contentStreamId = $contentStreamId AND r.dimensionSpacePointHash = $dimensionSpacePointHash)');
        $query->returns('nodes(path) as nodes');

        $result = $this->client->runStatement(
            $query->build()
        );
        $nodeTypeCriteria = null;
        if (!empty($filter->nodeTypes)) {
            $nodeTypeCriteria = ExpandedNodeTypeCriteria::create(
                $filter->nodeTypes,
                $this->nodeTypeManager,
            );
        }
        $results = [];

        foreach ($result->toArray() as $resultMap) {
            $nodes = $resultMap->getAsCypherList('nodes');
            foreach ($nodes as $distance => $node) {
                if (!array_key_exists($distance, $results)) {
                    $results[$distance] = [];
                }
                if (array_key_exists($node->getId(), $results[$distance])) {
                    continue; // Skip if node already exists at this distance
                }
                // We have to filter after the request due to the nature of neo4j-paths
                // TODO: Look into gds.path.filter()
                if ($nodeTypeCriteria?->matches(NodeTypeName::fromString($node->getProperty('nodeTypeName'))) === false) {
                    continue; // Skip if node type does not match criteria
                }
                if (!empty($filter->searchTerm) && !(
                    str_contains($node->getProperty('name'), $filter->searchTerm->term) ||
                    array_reduce(
                        $node->getProperties(),
                        fn(bool $carry, $propertyValue) => $carry || (is_string($propertyValue) && str_contains($propertyValue, $filter->searchTerm->term)),
                        false
                    )
                )) {
                    continue; // Skip if search term does not match
                }
                $results[$distance][$node->getId()] = $node;
            }
        }

        return $this->nodeFactory->mapResultToNodes(
            array_merge(...$results),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function countDescendantNodes(NodeAggregateId $entryNodeAggregateId, Filter\CountDescendantNodesFilter $filter): int
    {
        if ($this->debug) \Neos\Flow\var_dump('countDescendantNodes called with entryNodeAggregateId: ' . $entryNodeAggregateId->value);
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (:Node {aggregateId: $aggregateId})<-[:IS_CHILD*1.. {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]-(descendant:Node) RETURN count(DISTINCT descendant) as count',
                [
                    'aggregateId' => $entryNodeAggregateId->value,
                    'contentStreamId' => $this->contentStreamId->value,
                    'dimensionSpacePointHash' => $this->dimensionSpacePoint->hash,
                ]
            )
        );

        if ($result->isEmpty() || !$result->hasKey(0)) {
            return 0;
        }
        return $result->getAsCypherMap(0)->getAsInt('count');
    }

    public function findSubtree(NodeAggregateId $entryNodeAggregateId, Filter\FindSubtreeFilter $filter): ?Subtree
    {
        if ($this->debug) \Neos\Flow\var_dump('findSubtree called with entryNodeAggregateId: ' . $entryNodeAggregateId->value);
        $maxLevels = $filter->maximumLevels ?? 10; // Default to reasonable depth
        $query = NodeQueryBuilder::createForNodes()
            ->matchNodeForSubgraph(
                $this->contentStreamId,
                $this->dimensionSpacePoint,
                $entryNodeAggregateId,
            )
            ->match(sprintf('path = (n:Node)<-[rels:IS_CHILD*0..%d]-(descendant)', $maxLevels));

        $query->whereAll(
            function(QueryBuilder $qb) {
                $qb
                    ->where('r.contentStreamId = $contentStreamId')
                    ->withParameter('contentStreamId', $this->contentStreamId->value)
                    ->where('r.dimensionSpacePointHash = $dimensionSpacePointHash')
                    ->withParameter('dimensionSpacePointHash', $this->dimensionSpacePoint->hash);
                foreach ($this->visibilityConstraints as $constraint) {
                    foreach ($constraint as $subtreeTag) {
                        $qb->where(sprintf('COALESCE(apoc.convert.fromJsonMap(r.subtreeTags).%s, false) <> true', $subtreeTag->value));
                    }
                }
                return $qb;
            },
        );

        if (!empty($filter->nodeTypes)) {
            $expandedNodeTypeCriteria = ExpandedNodeTypeCriteria::create($filter->nodeTypes, $this->nodeTypeManager);

            if (!$expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames, 'descendant');
            }
            if (!$expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames, 'descendant', 'disallowedNodeTypeNames', negate: true);
            }
        }
        $query->returns('path');
        $result = $this->client->runStatement($query->build());

        if ($result->isEmpty()) {
            return null;
        }

        return $this->transformPathsToSubtree($result, $filter);
    }

    private function transformPathsToSubtree(SummarizedResult $result, Filter\FindSubtreeFilter $filter): ?Subtree
    {
        // Build a tree structure from the paths
        $nodesByAggregateId = [];
        $childrenByParentId = [];
        $childPositionsByParentId = [];
        $rootNodeAggregateId = null;

        // Process each path to extract nodes and relationships
        foreach ($result as $record) {
            $path = $record->getAsPath('path');
            $nodes = $path->getNodes();
            $relationships = $path->getRelationships();

            // First node in path is the root (entry node)
            if ($rootNodeAggregateId === null && !$nodes->isEmpty()) {
                $rootNodeAggregateId = $nodes->first()->getProperty('aggregateId');
            }

            // Store all nodes by their aggregate ID
            foreach ($nodes as $node) {
                $aggregateId = $node->getProperty('aggregateId');
                if (!isset($nodesByAggregateId[$aggregateId])) {
                    $nodesByAggregateId[$aggregateId] = $this->nodeFactory->mapResultToNode(
                        $node,
                        $this->workspaceName,
                        $this->dimensionSpacePoint,
                        $this->visibilityConstraints
                    );
                }
            }

            // Build parent-child relationships from the path with position information
            $pathNodes = $nodes->toArray();
            $pathRelationships = $relationships->toArray();

            for ($i = 0; $i < count($pathNodes) - 1; $i++) {
                $parentId = $pathNodes[$i]->getProperty('aggregateId');
                $childId = $pathNodes[$i + 1]->getProperty('aggregateId');

                if (!isset($childrenByParentId[$parentId])) {
                    $childrenByParentId[$parentId] = [];
                    $childPositionsByParentId[$parentId] = [];
                }

                if (!in_array($childId, $childrenByParentId[$parentId])) {
                    $childrenByParentId[$parentId][] = $childId;

                    // Extract position from the relationship if available
                    $position = 0;
                    if (isset($pathRelationships[$i])) {
                        $relationshipProperties = $pathRelationships[$i]->getProperties();
                        if ($relationshipProperties->offsetExists('position')) {
                            $position = $relationshipProperties->get('position');
                        }
                    }
                    $childPositionsByParentId[$parentId][$childId] = $position;
                }
            }
        }

        // If no root node found, return null
        if ($rootNodeAggregateId === null || !isset($nodesByAggregateId[$rootNodeAggregateId])) {
            return null;
        }

        // Build the subtree recursively starting from the root
        return $this->buildSubtreeFromNodes(
            $nodesByAggregateId[$rootNodeAggregateId],
            0,
            $nodesByAggregateId,
            $childrenByParentId,
            $childPositionsByParentId,
            $filter
        );
    }

    private function buildSubtreeFromNodes(
        Node $node,
        int $level,
        array $nodesByAggregateId,
        array $childrenByParentId,
        array $childPositionsByParentId,
        Filter\FindSubtreeFilter $filter
    ): Subtree {
        // Apply maximum level filter if specified
        if ($filter->maximumLevels !== null && $level >= $filter->maximumLevels) {
            return Subtree::create($level, $node, Subtrees::createEmpty());
        }

        // Get children for this node
        $childAggregateIds = $childrenByParentId[$node->aggregateId->value] ?? [];
        $childPositions = $childPositionsByParentId[$node->aggregateId->value] ?? [];

        // Sort children by position
        if (!empty($childPositions)) {
            usort($childAggregateIds, function($a, $b) use ($childPositions) {
                $positionA = $childPositions[$a] ?? 0;
                $positionB = $childPositions[$b] ?? 0;
                return $positionA <=> $positionB;
            });
        }

        $childSubtrees = [];

        foreach ($childAggregateIds as $childAggregateId) {
            if (isset($nodesByAggregateId[$childAggregateId])) {
                $childNode = $nodesByAggregateId[$childAggregateId];

                // Apply node type filtering if specified
                if ($this->shouldIncludeNodeInSubtree($childNode, $filter)) {
                    $childSubtrees[] = $this->buildSubtreeFromNodes(
                        $childNode,
                        $level + 1,
                        $nodesByAggregateId,
                        $childrenByParentId,
                        $childPositionsByParentId,
                        $filter
                    );
                }
            }
        }

        return Subtree::create($level, $node, Subtrees::fromArray($childSubtrees));
    }

    private function shouldIncludeNodeInSubtree(Node $node, Filter\FindSubtreeFilter $filter): bool
    {
        // Apply node type filtering if specified
        if ($filter->nodeTypes !== null) {
            $nodeTypeCriteria = ExpandedNodeTypeCriteria::create(
                $filter->nodeTypes,
                $this->nodeTypeManager
            );

            if (!$nodeTypeCriteria->matches($node->nodeTypeName)) {
                return false;
            }
        }

        return true;
    }

    public function findReferences(NodeAggregateId $nodeAggregateId, Filter\FindReferencesFilter $filter): References
    {
        if ($this->debug) \Neos\Flow\var_dump('findReferences called with nodeAggregateId: ' . $nodeAggregateId->value);
        $query = $this->getReferencesQuery(false, $nodeAggregateId, $filter);
        $query->returns('target, ref');
        $result = $this->client->runStatement($query->build());

        return References::fromArray(array_map(fn(CypherMap $map) => (
            $this->nodeFactory->mapResultToReference(
                $map->getAsNode('target'),
                $this->workspaceName,
                $this->dimensionSpacePoint,
                $this->visibilityConstraints,
                $map->getAsRelationship('ref'),
            )), $result->toArray()));
    }


    public function countReferences(NodeAggregateId $nodeAggregateId, Filter\CountReferencesFilter $filter): int
    {
        if ($this->debug) \Neos\Flow\var_dump('countReferences called with nodeAggregateId: ' . $nodeAggregateId->value);
        $query = $this->getReferencesQuery(false, $nodeAggregateId, $filter);
        $query->returns('COUNT(DISTINCT target AS count');
        return $this->client->runStatement($query->build())->getAsCypherMap(0)->getAsInt('count');
    }

    public function findBackReferences(NodeAggregateId $nodeAggregateId, Filter\FindBackReferencesFilter $filter): References
    {
        if ($this->debug) \Neos\Flow\var_dump('findBackReferences called with nodeAggregateId: ' . $nodeAggregateId->value);
        $query = $this->getReferencesQuery(true, $nodeAggregateId, $filter);
        $query->returns('target, ref');
        $result = $this->client->runStatement($query->build());
        return References::fromArray(array_map(fn(CypherMap $map) => (
        $this->nodeFactory->mapResultToReference(
            $map->getAsNode('target'),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
            $map->getAsRelationship('ref'),
        )), $result->toArray()));
    }

    public function countBackReferences(NodeAggregateId $nodeAggregateId, Filter\CountBackReferencesFilter $filter): int
    {
        if ($this->debug) \Neos\Flow\var_dump('countBackReferences called with nodeAggregateId: ' . $nodeAggregateId->value);
        $query = $this->getReferencesQuery(true, $nodeAggregateId, $filter);
        $query->returns('COUNT(DISTINCT target AS count');
        return $this->client->runStatement($query->build())->getAsCypherMap(0)->getAsInt('count');
    }

    private function getReferencesQuery(
        bool $backreferences,
        NodeAggregateId $aggregateId,
        Filter\FindReferencesFilter|Filter\FindBackReferencesFilter|Filter\CountReferencesFilter|Filter\CountBackReferencesFilter $filter
    ): NodeQueryBuilder {
        $query = NodeQueryBuilder::createForNodes();
        if ($backreferences) {
            $query->match('(target:Node)-[ref:REFERENCE]->(source:Node)');
        } else {
            $query->match('(source:Node)-[ref:REFERENCE]->(target:Node)');
        }
        $query->match('(source)-[sourceChild:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(:Node)');
        $query->match('(target)-[targetChild:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(:Node)');

        foreach ($this->visibilityConstraints as $constraint) {
            foreach ($constraint as $subtreeTag) {
                $query->where(sprintf('COALESCE(apoc.convert.fromJsonMap(sourceChild.subtreeTags).%s, false) <> true', $subtreeTag->value));
                $query->where(sprintf('COALESCE(apoc.convert.fromJsonMap(targetChild.subtreeTags).%s, false) <> true', $subtreeTag->value));
            }
        }

        $query->match('(source)-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(:Node)');
        $query->match('(target)-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->(:Node)');
        $query->matchNodeByAggregateId($aggregateId, 'source');
        $query->withParameters([
            'contentStreamId' => $this->contentStreamId->value,
            'dimensionSpacePointHash' => $this->dimensionSpacePoint->hash,
        ]);

        if ($filter->nodeTypes) {
            $expandedNodeTypeCriteria = ExpandedNodeTypeCriteria::create($filter->nodeTypes, $this->nodeTypeManager);
            if (!$expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyAllowedNodeTypeNames, 'target');
            }
            if (!$expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames->isEmpty()) {
                $query->whereNodeTypeIn($expandedNodeTypeCriteria->explicitlyDisallowedNodeTypeNames, 'target', negate: true);
            }
        }

        if ($filter->nodeSearchTerm) {
            $query->where('(target.name CONTAINS $searchTerm OR any(prop in keys(target.properties) WHERE target.properties[prop] CONTAINS $searchTerm))')
                ->withParameter('searchTerm', $filter->nodeSearchTerm->term);
        }
        if ($filter->nodePropertyValue) {
            $propertyParams = [];
            $propertyCondition = $this->buildPropertyValueCondition($filter->nodePropertyValue, 'target', $propertyParams);
            if ($propertyCondition !== null) {
                $query
                    ->where($propertyCondition)
                    ->withParameters($propertyParams);
            }
        }
        if ($filter->referenceSearchTerm) {
            $query->where('(target.name CONTAINS $referenceSearchTerm OR any(prop in keys(target.properties) WHERE target.properties[prop] CONTAINS $referenceSearchTerm))')
                ->withParameter('referenceSearchTerm', $filter->referenceSearchTerm->term);
        }
        if ($filter->referencePropertyValue) {
            $propertyParams = [];
            $propertyCondition = $this->buildPropertyValueCondition($filter->referencePropertyValue, 'ref', $propertyParams);
            if ($propertyCondition !== null) {
                $query
                    ->where($propertyCondition)
                    ->withParameters($propertyParams);
            }
        }
        if ($filter->referenceName) {
            $query->where('ref.referenceName = $referenceName')
                ->withParameter('referenceName', $filter->referenceName->value);
        }
        if ($filter instanceof Filter\FindReferencesFilter || $filter instanceof Filter\FindBackReferencesFilter) {
            if ($filter->ordering !== null) {
                foreach ($filter->ordering as $ordering) {
                    $query->orderBy('ref.'.$ordering->field->value, $ordering->direction->value);
                }
            } elseif ($filter->referenceName === null) {
                $query->orderBy('ref.referenceName');
            }
            $query->orderBy('ref.position')
                ->orderBy('target.aggregateid');
            if ($filter->pagination !== null) {
                $query->limit($filter->pagination->limit)
                    ->skip($filter->pagination->offset);
            }
        }
        return $query;
    }

    public function findNodeByPath(NodeName|NodePath $path, NodeAggregateId $startingNodeAggregateId): ?Node
    {
        if ($this->debug) \Neos\Flow\var_dump('findNodeByPath called with path: ' . ' __ insert path here... __ ' . ' and startingNodeAggregateId: ' . $startingNodeAggregateId->value);
        $path = $path instanceof NodeName ? NodePath::fromNodeNames($path) : $path;

        return $this->findNodeByPathFromStartingNode($path, $startingNodeAggregateId);
    }

    public function findNodeByAbsolutePath(AbsoluteNodePath $path): ?Node
    {
        if ($this->debug) \Neos\Flow\var_dump('findNodeByAbsolutePath called with path: ' . $path->path . ' and rootNodeTypeName: ' . $path->rootNodeTypeName);
        $startingNode = $this->findRootNodeByType($path->rootNodeTypeName);

        return $startingNode
            ? $this->findNodeByPathFromStartingNode($path->path, $startingNode)
            : null;
    }

    private function findNodeByPathFromStartingNode(NodePath $path, Node|NodeAggregateId $startingNode): ?Node
    {
        $query = NodeQueryBuilder::createForNodes()
            ->matchNodeForSubgraph(
                $this->contentStreamId,
                $this->dimensionSpacePoint,
                $startingNode,
                nodeAlias: 'startingNode',
            );

        $query->rawClause('MATCH path = ');
        $lastNodeAlias = 'startingNode';
        $highestIndex = -1;
        foreach ($path->getParts() as $part) {
            if ($part->value === 'sites') continue;
            $highestIndex++;
            if ($highestIndex === 0) {
                $query->rawClause(sprintf('(%s)<-[childRel%s]-(childNode%s)', $lastNodeAlias, $highestIndex, $highestIndex));
            } else {
                $query->match(sprintf('(%s)<-[childRel%s]-(childNode%s)', $lastNodeAlias, $highestIndex, $highestIndex));
            }
            $query
                ->where(sprintf('childNode%s.name = $name%s', $highestIndex, $highestIndex))
                ->withParameter('name' . $highestIndex, $part->value)
                ->where(sprintf('childRel%s.contentStreamId = $contentStreamId', $highestIndex))
                ->where(sprintf('childRel%s.dimensionSpacePointHash = $dimensionSpacePointHash', $highestIndex));
            foreach ($this->visibilityConstraints as $constraint) {
                foreach ($constraint as $subtreeTag) {
                    $query->where(sprintf('COALESCE(apoc.convert.fromJsonMap(childRel%s.subtreeTags).%s, false) <> true', $highestIndex, $subtreeTag->value));
                }
            }

            $lastNodeAlias = sprintf('childNode%s', $highestIndex);
        }
        $query->returns(sprintf('childNode%s as node, childRel%s as rel', $highestIndex, $highestIndex));

        $result = $this->client->runStatement($query->build());

        if ($result->isEmpty() || !$result->hasKey(0)) {
            return null;
        }

        return $this->nodeFactory->mapResultToNode(
            $result->getAsCypherMap(0),
            $this->workspaceName,
            $this->dimensionSpacePoint,
            $this->visibilityConstraints,
        );
    }

    public function retrieveNodePath(NodeAggregateId $nodeAggregateId): AbsoluteNodePath
    {
        if ($this->debug) \Neos\Flow\var_dump('retrieveNodePath called with nodeAggregateId: ' . $nodeAggregateId->value);
        // TODO: Implement retrieveNodePath() method.
        throw new \RuntimeException('Not implemented yet');
    }

    public function countNodes(): int
    {
        if ($this->debug) \Neos\Flow\var_dump('countNodes called');
        /** @var SummarizedResult $result */
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (n)-[:IS_CHILD {contentStreamId: $contentStreamId, dimensionSpacePointHash: $dimensionSpacePointHash}]->() RETURN count(DISTINCT n) AS nodeCount',
                [
                    'contentStreamId' => $this->contentStreamId->value,
                    'dimensionSpacePointHash' => $this->dimensionSpacePoint->hash,
                ]
            )
        );

        return $result->getAsCypherMap(0)->getAsInt('nodeCount');
    }

    private function buildPropertyValueCondition(Filter\PropertyValue\Criteria\PropertyValueCriteriaInterface $criteria, string $nodeVariable, array &$queryParams): ?string
    {
        return match (get_class($criteria)) {
            Filter\PropertyValue\Criteria\PropertyValueEquals::class => $this->buildPropertyValueEqualsCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueContains::class => $this->buildPropertyValueContainsCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueStartsWith::class => $this->buildPropertyValueStartsWithCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueEndsWith::class => $this->buildPropertyValueEndsWithCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueGreaterThan::class => $this->buildPropertyValueGreaterThanCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueGreaterThanOrEqual::class => $this->buildPropertyValueGreaterThanOrEqualCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueLessThan::class => $this->buildPropertyValueLessThanCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\PropertyValueLessThanOrEqual::class => $this->buildPropertyValueLessThanOrEqualCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\AndCriteria::class => $this->buildAndCriteriaCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\OrCriteria::class => $this->buildOrCriteriaCondition($criteria, $nodeVariable, $queryParams),
            Filter\PropertyValue\Criteria\NegateCriteria::class => $this->buildNegateCriteriaCondition($criteria, $nodeVariable, $queryParams),
            default => null,
        };
    }

    private function buildPropertyValueEqualsCondition(Filter\PropertyValue\Criteria\PropertyValueEquals $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        if ($criteria->caseSensitive) {
            return sprintf('%s.properties["%s"] = $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
        } else {
            return sprintf('toLower(toString(%s.properties["%s"])) = toLower(toString($%s))', $nodeVariable, $criteria->propertyName->value, $paramName);
        }
    }

    private function buildPropertyValueContainsCondition(Filter\PropertyValue\Criteria\PropertyValueContains $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        if ($criteria->caseSensitive) {
            return sprintf('toString(%s.properties["%s"]) CONTAINS $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
        } else {
            return sprintf('toLower(toString(%s.properties["%s"])) CONTAINS toLower($%s)', $nodeVariable, $criteria->propertyName->value, $paramName);
        }
    }

    private function buildPropertyValueStartsWithCondition(Filter\PropertyValue\Criteria\PropertyValueStartsWith $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        if ($criteria->caseSensitive) {
            return sprintf('toString(%s.properties["%s"]) STARTS WITH $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
        } else {
            return sprintf('toLower(toString(%s.properties["%s"])) STARTS WITH toLower($%s)', $nodeVariable, $criteria->propertyName->value, $paramName);
        }
    }

    private function buildPropertyValueEndsWithCondition(Filter\PropertyValue\Criteria\PropertyValueEndsWith $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        if ($criteria->caseSensitive) {
            return sprintf('toString(%s.properties["%s"]) ENDS WITH $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
        } else {
            return sprintf('toLower(toString(%s.properties["%s"])) ENDS WITH toLower($%s)', $nodeVariable, $criteria->propertyName->value, $paramName);
        }
    }

    private function buildPropertyValueGreaterThanCondition(Filter\PropertyValue\Criteria\PropertyValueGreaterThan $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        return sprintf('%s.properties["%s"] > $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
    }

    private function buildPropertyValueGreaterThanOrEqualCondition(Filter\PropertyValue\Criteria\PropertyValueGreaterThanOrEqual $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        return sprintf('%s.properties["%s"] >= $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
    }

    private function buildPropertyValueLessThanCondition(Filter\PropertyValue\Criteria\PropertyValueLessThan $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        return sprintf('%s.properties["%s"] < $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
    }

    private function buildPropertyValueLessThanOrEqualCondition(Filter\PropertyValue\Criteria\PropertyValueLessThanOrEqual $criteria, string $nodeVariable, array &$queryParams): string
    {
        $paramName = 'propValue_' . uniqid();
        $queryParams[$paramName] = $criteria->value;

        return sprintf('%s.properties["%s"] <= $%s', $nodeVariable, $criteria->propertyName->value, $paramName);
    }

    private function buildAndCriteriaCondition(Filter\PropertyValue\Criteria\AndCriteria $criteria, string $nodeVariable, array &$queryParams): string
    {
        $condition1 = $this->buildPropertyValueCondition($criteria->criteria1, $nodeVariable, $queryParams);
        $condition2 = $this->buildPropertyValueCondition($criteria->criteria2, $nodeVariable, $queryParams);

        $conditions = [];
        if ($condition1 !== null) {
            $conditions[] = $condition1;
        }
        if ($condition2 !== null) {
            $conditions[] = $condition2;
        }

        return '(' . implode(' AND ', $conditions) . ')';
    }

    private function buildOrCriteriaCondition(Filter\PropertyValue\Criteria\OrCriteria $criteria, string $nodeVariable, array &$queryParams): string
    {
        $condition1 = $this->buildPropertyValueCondition($criteria->criteria1, $nodeVariable, $queryParams);
        $condition2 = $this->buildPropertyValueCondition($criteria->criteria2, $nodeVariable, $queryParams);

        $conditions = [];
        if ($condition1 !== null) {
            $conditions[] = $condition1;
        }
        if ($condition2 !== null) {
            $conditions[] = $condition2;
        }

        return '(' . implode(' OR ', $conditions) . ')';
    }

    private function buildNegateCriteriaCondition(Filter\PropertyValue\Criteria\NegateCriteria $criteria, string $nodeVariable, array &$queryParams): string
    {
        $condition = $this->buildPropertyValueCondition($criteria->criteria, $nodeVariable, $queryParams);
        return $condition !== null ? 'NOT (' . $condition . ')' : '';
    }

    private function buildOrderingFieldExpression(Filter\Ordering\OrderingField $orderingField, string $nodeVariable): ?string
    {
        if ($orderingField->field instanceof \Neos\ContentRepository\Core\SharedModel\Node\PropertyName) {
            return sprintf('%s.properties["%s"]', $nodeVariable, $orderingField->field->value);
        } elseif ($orderingField->field instanceof Filter\Ordering\TimestampField) {
            return match ($orderingField->field) {
                Filter\Ordering\TimestampField::CREATED => $nodeVariable . '.created',
                Filter\Ordering\TimestampField::ORIGINAL_CREATED => $nodeVariable . '.originalCreated',
                Filter\Ordering\TimestampField::LAST_MODIFIED => $nodeVariable . '.lastModified',
                Filter\Ordering\TimestampField::ORIGINAL_LAST_MODIFIED => $nodeVariable . '.originalLastModified',
            };
        }

        return null;
    }
}
