<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query;

use Laudis\Neo4j\Types\Node;
use Neos\ContentRepository\Core\NodeType\NodeTypeNames;
use Neos\ContentRepository\Core\Projection\ContentGraph\VisibilityConstraints;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\ContentRepository\Core\NodeType\NodeTypeName;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;

/**
 * Specialized query builder for Node-related queries
 *
 * @internal
 */
final class NodeQueryBuilder extends QueryBuilder
{
    public static function createForNodes(): self
    {
        return new self();
    }

    public function matchNodesByType(NodeTypeName $nodeTypeName, string $alias = 'n'): self
    {
        return $this->match("({$alias}:Node {nodeTypeName: \$nodeTypeName})")
                   ->withParameter('nodeTypeName', $nodeTypeName->value);
    }

    public function matchNodeByAggregateId(NodeAggregateId $nodeAggregateId, string $alias = 'n'): self
    {
        return $this->match("({$alias}:Node {aggregateId: \$aggregateId})")
                   ->withParameter('aggregateId', $nodeAggregateId->value);
    }

    public function matchNodeForSubgraph(
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        Node|NodeAggregateId $nodeAggregateId,
        string $nodeAlias = 'n',
        string $relationAlias = 'rel',
        string $parentAlias = 'p',
    ): self {
        return $this->match("({$nodeAlias}:Node {aggregateId: \$aggregateId})-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId, dimensionSpacePointHash: \$dimensionSpacePointHash}]->({$parentAlias}:Node|Root)")
           ->withParameter(
               'aggregateId',
               $nodeAggregateId instanceof NodeAggregateId ? $nodeAggregateId->value : $nodeAggregateId->getProperty('aggregateId')
           )
           ->withParameter('contentStreamId', $contentStreamId->value)
           ->withParameter('dimensionSpacePointHash', $dimensionSpacePoint->hash);
    }

    public function withVisibilityConstraints(
        VisibilityConstraints $visibilityConstraints,
        string $relationAlias= 'rel',
    ): self
    {
        foreach ($visibilityConstraints as $constraint) {
            foreach ($constraint as $subtreeTag) {
                $this->where(sprintf('COALESCE(apoc.convert.fromJsonMap(%s.subtreeTags).%s, false) = false', $relationAlias, $subtreeTag->value));
            }
        }
        return $this;
    }

    public function matchNodeForContentStream(
        ContentStreamId $contentStreamId,
        NodeAggregateId $nodeAggregateId,
        string $nodeAlias = 'n',
        string $relationAlias = 'rel'
    ): self
    {
        return $this->match("({$nodeAlias}:Node {aggregateId: \$aggregateId})-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId}]->()")
            ->withParameter('aggregateId', $nodeAggregateId->value)
            ->withParameter('contentStreamId', $contentStreamId->value);
    }

    public function matchNodesForContentStream(
        ContentStreamId $contentStreamId,
        string $nodeAlias = 'n',
        string $relationAlias = 'rel'
    ): self {
        return $this->match("({$nodeAlias}:Node)-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId}]->(:Node|Root)")
            ->withParameter('contentStreamId', $contentStreamId->value);
    }

    public function matchChildrenForSubgraph(
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        Node|NodeAggregateId $parentNodeAggregateId,
        string $nodeAlias = 'child',
        string $relationAlias = 'rel',
        string $parentAlias = 'p'
    ): self {
        return $this->match("({$parentAlias}:Node|Root {aggregateId: \$aggregateId})<-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId, dimensionSpacePointHash: \$dimensionSpacePointHash}]-({$nodeAlias}:Node)")
           ->withParameter('aggregateId',
               $parentNodeAggregateId instanceof NodeAggregateId ? $parentNodeAggregateId->value : $parentNodeAggregateId->getProperty('aggregateId')
           )
           ->withParameter('contentStreamId', $contentStreamId->value)
           ->withParameter('dimensionSpacePointHash', $dimensionSpacePoint->hash);
    }

    public function matchParentNodeForSubgraph(
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        NodeAggregateId $nodeAggregateId,
        string $parentAlias = 'p'
    ): self {
        return $this->match("(:Node {aggregateId: \$aggregateId})-[:IS_CHILD]->(parent:Node)")
            ->match("({$parentAlias}:Node {aggregateId: parent.aggregateId})-[:IS_CHILD {contentStreamId: \$contentStreamId, dimensionSpacePointHash: \$dimensionSpacePointHash}]->(:Node|Root)")
           ->withParameter('aggregateId', $nodeAggregateId->value)
           ->withParameter('contentStreamId', $contentStreamId->value)
           ->withParameter('dimensionSpacePointHash', $dimensionSpacePoint->hash);
    }
    public function matchNodeWithChildRelation(
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        string $nodeAlias = 'n',
        string $relationAlias = 'rel',
        string $parentAlias = 'p'
    ): self {
        return $this->match("({$nodeAlias}:Node)-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId, dimensionSpacePointHash: \$dimensionSpacePointHash}]->({$parentAlias}:Node)")
                   ->withParameter('contentStreamId', $contentStreamId->value)
                   ->withParameter('dimensionSpacePointHash', $dimensionSpacePoint->hash);
    }

    public function matchRootPath(
        NodeAggregateId $nodeAggregateId,
        ContentStreamId $contentStreamId,
        DimensionSpacePoint $dimensionSpacePoint,
        string $pathAlias = 'path',
        string $nodeAlias = '',
        string $relationAlias = ''
    ): self {
        return $this->match("{$pathAlias} = ({$nodeAlias}:Node {aggregateId: \$aggregateId})-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId, dimensionSpacePointHash: \$dimensionSpacePointHash}]-+(:Root)")
                   ->withParameter('aggregateId', $nodeAggregateId->value)
                   ->withParameter('contentStreamId', $contentStreamId->value)
                   ->withParameter('dimensionSpacePointHash', $dimensionSpacePoint->hash);
    }

    public function matchNodeWithInvertedChildRelation(
        ContentStreamId $contentStreamId,
        string $nodeAlias = 'n',
        string $relationAlias = 'rel'
    ): self {
        return $this->match("({$nodeAlias}:Node)<-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId}]-()")
                   ->withParameter('contentStreamId', $contentStreamId->value);
    }

    public function matchNodeWithRootRelation(
        ContentStreamId $contentStreamId,
        string $nodeAlias = 'n',
        string $relationAlias = 'rel'
    ): self {
        return $this->match("({$nodeAlias}:Node)-[{$relationAlias}:IS_CHILD {contentStreamId: \$contentStreamId}]->(:Root)")
                   ->withParameter('contentStreamId', $contentStreamId->value);
    }

    public function whereNodeInDimensionSpace(
        string $dimensionSpacePointHash,
        string $relationAlias = 'rel'
    ): self {
        return $this->where("{$relationAlias}.dimensionSpacePointHash = \$dimensionSpacePointHash")
                   ->withParameter('dimensionSpacePointHash', $dimensionSpacePointHash);
    }

    public function returnStandardNodeFields(string $nodeAlias = 'n', string $relationAlias = 'rel'): self
    {
        return $this->returns(
            "{$nodeAlias}.aggregateId as aggregateId, " .
            "{$nodeAlias}.nodeTypeName as nodeTypeName, " .
            "{$nodeAlias}.name as name, " .
            "{$nodeAlias}.classification as classification, " .
            "{$nodeAlias}.originDimensionSpacePointHash as originDimensionSpacePointHash, " .
            "{$nodeAlias}.created as created, " .
            "{$nodeAlias}.originalCreated as originalCreated, " .
            "{$nodeAlias}.lastModified as lastModified, " .
            "{$nodeAlias}.originalLastModified as originalLastModified, " .
            "{$nodeAlias}.properties as properties, " .
            "{$relationAlias}.dimensionSpacePointHash as dimensionSpacePointHash, " .
            "{$relationAlias}.contentStreamId as contentStreamId, " .
            "{$relationAlias}.position as position, " .
            "{$relationAlias}.subtreeTags as subtreeTags"
        );
    }


    public function whereNodeTypeIn(NodeTypeNames|array $nodeTypeNames, string $nodeAlias = 'n', string $parameterAlias = 'allowedNodeTypes', bool $negate = false): self
    {
        return $this
            ->where(($negate ? "NOT " : "") . "{$nodeAlias}.nodeTypeName IN \${$parameterAlias}")
            ->withParameter($parameterAlias, array_values($nodeTypeNames instanceof NodeTypeNames ? $nodeTypeNames->toStringArray() : $nodeTypeNames));
    }

    public function whereContentStream(ContentStreamId $contentStreamId, string $relationAlias = 'rel'): self
    {
        return $this->where("{$relationAlias}.contentStreamId = \$contentStreamId")
                   ->withParameter('contentStreamId', $contentStreamId->value);
    }

    public function orderByNodeName(string $direction = 'ASC', string $nodeAlias = 'n'): self
    {
        return $this->orderBy("{$nodeAlias}.name", $direction);
    }

    public function orderByCreated(string $direction = 'ASC', string $nodeAlias = 'n'): self
    {
        return $this->orderBy("{$nodeAlias}.created", $direction);
    }

    public function limitResults(int $limit): self
    {
        return $this->limit($limit);
    }

    public function setProperty(string $propertyName, mixed $value, string $nodeAlias = 'n'): self
    {
        return $this->set("{$nodeAlias}.{$propertyName} = \${$propertyName}_propertyValue")
                   ->withParameter($propertyName . '_propertyValue', $value);
    }
}
