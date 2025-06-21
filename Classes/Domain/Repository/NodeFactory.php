<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository;

use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Laudis\Neo4j\Types\Relationship;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePoint;
use Neos\ContentRepository\Core\DimensionSpace\DimensionSpacePointSet;
use Neos\ContentRepository\Core\DimensionSpace\OriginDimensionSpacePoint;
use Neos\ContentRepository\Core\DimensionSpace\OriginDimensionSpacePointSet;
use Neos\ContentRepository\Core\Feature\NodeModification\Dto\SerializedPropertyValues;
use Neos\ContentRepository\Core\Infrastructure\Property\PropertyConverter;
use Neos\ContentRepository\Core\NodeType\NodeTypeName;
use Neos\ContentRepository\Core\Projection\ContentGraph\CoverageByOrigin;
use Neos\ContentRepository\Core\Projection\ContentGraph\Node;
use Neos\ContentRepository\Core\Projection\ContentGraph\NodeAggregate;
use Neos\ContentRepository\Core\Projection\ContentGraph\Nodes;
use Neos\ContentRepository\Core\Projection\ContentGraph\NodeAggregates;
use Neos\ContentRepository\Core\Projection\ContentGraph\NodeTags;
use Neos\ContentRepository\Core\Projection\ContentGraph\OriginByCoverage;
use Neos\ContentRepository\Core\Projection\ContentGraph\PropertyCollection;
use Neos\ContentRepository\Core\Projection\ContentGraph\Reference;
use Neos\ContentRepository\Core\Projection\ContentGraph\Timestamps;
use Neos\ContentRepository\Core\Projection\ContentGraph\VisibilityConstraints;
use Neos\ContentRepository\Core\SharedModel\ContentRepository\ContentRepositoryId;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateClassification;
use Neos\ContentRepository\Core\SharedModel\Node\NodeAggregateId;
use Neos\ContentRepository\Core\SharedModel\Node\NodeName;
use Neos\ContentRepository\Core\SharedModel\Node\ReferenceName;
use Neos\ContentRepository\Core\SharedModel\Workspace\WorkspaceName;

/**
 * Factory for creating NodeAggregate objects from Neo4j query results
 *
 * @internal
 */
final class NodeFactory
{
    public function __construct(
        private readonly ContentRepositoryId $contentRepositoryId,
        private readonly Neo4jDimensionSpacePointsRepository $dimensionSpacePointsRepository,
        private readonly PropertyConverter $propertyConverter,
    ) {
    }

    /**
     * Maps Neo4j query results to NodeAggregates (multiple node aggregates)
     */
    public function mapResultToNodeAggregates(
        SummarizedResult $result,
        WorkspaceName $workspaceName,
    ): NodeAggregates {
        if ($result->count() === 0) {
            return NodeAggregates::createEmpty();
        }

        // Group records by node aggregate ID
        $recordsByNodeAggregateId = [];
        foreach ($result as $record) {
            $nodeAggregateId = $record->get('aggregateId');
            if (!isset($recordsByNodeAggregateId[$nodeAggregateId])) {
                $recordsByNodeAggregateId[$nodeAggregateId] = [];
            }
            $recordsByNodeAggregateId[$nodeAggregateId][] = $record;
        }

        $nodeAggregates = [];
        foreach ($recordsByNodeAggregateId as $nodeAggregateId => $records) {
            $nodeAggregate = $this->createNodeAggregateFromRecords($records, $workspaceName);
            if ($nodeAggregate !== null) {
                $nodeAggregates[] = $nodeAggregate;
            }
        }

        return NodeAggregates::fromArray($nodeAggregates);
    }

    /**
     * Maps Neo4j query results to a single NodeAggregate (assuming all records belong to the same aggregate)
     */
    public function mapResultToNodeAggregate(
        SummarizedResult $result,
        WorkspaceName $workspaceName,
    ): ?NodeAggregate {
        if ($result->count() === 0) {
            return null;
        }

        // Convert all records to array and create a single NodeAggregate
        $records = [];
        foreach ($result as $record) {
            $records[] = $record;
        }

        return $this->createNodeAggregateFromRecords($records, $workspaceName);
    }

    /**
     * Creates a NodeAggregate from an array of records that belong to the same aggregate
     */
    private function createNodeAggregateFromRecords(array $records, WorkspaceName $workspaceName): ?NodeAggregate
    {
        if (empty($records)) {
            return null;
        }

        $firstRecord = $records[0];
        $nodeAggregateId = NodeAggregateId::fromString($firstRecord->get('aggregateId'));
        $classification = NodeAggregateClassification::from($firstRecord->get('classification'));
        $nodeTypeName = NodeTypeName::fromString($firstRecord->get('nodeTypeName'));
        $nodeName = $firstRecord->get('name') ? NodeName::fromString($firstRecord->get('name')) : null;

        // Build dimension space point collections from all records
        $occupiedDimensionSpacePoints = [];
        $nodesByOccupiedDimensionSpacePoint = [];
        $coveredDimensionSpacePoints = [];
        $coverageByOccupants = [];
        $occupationByCovering = [];
        $nodeTagsByCoveredDimensionSpacePoint = [];

        foreach ($records as $record) {
            $originDimensionSpacePointHash = $record->get('originDimensionSpacePointHash');
            $dimensionSpacePointHash = $record->get('dimensionSpacePointHash');

            // Resolve dimension space points from hashes
            $originDimensionSpacePoint = $this->resolveDimensionSpacePointFromHash($originDimensionSpacePointHash);
            $coveredDimensionSpacePoint = $this->resolveCoveredDimensionSpacePointFromHash($dimensionSpacePointHash);

            // Track occupied dimension space points
            if (!isset($occupiedDimensionSpacePoints[$originDimensionSpacePoint->hash])) {
                $occupiedDimensionSpacePoints[$originDimensionSpacePoint->hash] = $originDimensionSpacePoint;
                // Create Node object for this occupied dimension space point
                $nodesByOccupiedDimensionSpacePoint[$originDimensionSpacePoint->hash] = $this->mapResultToNode(
                    $record,
                    $workspaceName,
                    $originDimensionSpacePoint->toDimensionSpacePoint(),
                    VisibilityConstraints::createEmpty()
                );
            }

            // Track covered dimension space points
            $coveredDimensionSpacePoints[$coveredDimensionSpacePoint->hash] = $coveredDimensionSpacePoint;

            // Build coverage mappings
            $coverageByOccupants[$originDimensionSpacePoint->hash][$coveredDimensionSpacePoint->hash] = $coveredDimensionSpacePoint;
            $occupationByCovering[$coveredDimensionSpacePoint->hash] = $originDimensionSpacePoint;
            $nodeTagsByCoveredDimensionSpacePoint[$coveredDimensionSpacePoint->hash] = NodeTags::createEmpty();
        }

        return NodeAggregate::create(
            $this->contentRepositoryId,
            $workspaceName,
            $nodeAggregateId,
            $classification,
            $nodeTypeName,
            $nodeName,
            new OriginDimensionSpacePointSet(array_values($occupiedDimensionSpacePoints)),
            $nodesByOccupiedDimensionSpacePoint,
            CoverageByOrigin::fromArray($coverageByOccupants),
            new DimensionSpacePointSet($coveredDimensionSpacePoints),
            OriginByCoverage::fromArray($occupationByCovering),
            $nodeTagsByCoveredDimensionSpacePoint
        );
    }

    public function mapResultToNode(
        CypherMap|\Laudis\Neo4j\Types\Node $record,
        WorkspaceName $workspaceName,
        DimensionSpacePoint $dimensionSpacePoint,
        VisibilityConstraints $visibilityConstraints
    ): Node {
        if ($record instanceof \Laudis\Neo4j\Types\Node) {
            $record = $record->getProperties();
        }
        return Node::create(
            $this->contentRepositoryId,
            $workspaceName,
            $dimensionSpacePoint,
            NodeAggregateId::fromString($record->get('aggregateId')),
            $this->resolveDimensionSpacePointFromHash($record->get('originDimensionSpacePointHash')),
            NodeAggregateClassification::from($record->get('classification')),
            NodeTypeName::fromString($record->get('nodeTypeName')),
            $this->createPropertyCollectionFromJsonString($record->hasKey('properties') ? ($record->get('properties') ?: '{}') : '{}'),
            ($record->hasKey('name') && !empty($record->get('name'))) ? NodeName::fromString($record->get('name')) : null,
            NodeTags::createEmpty(), // TODO: Extract from Neo4j record if needed
            $this->createTimestampsFromRecord($record),
            $visibilityConstraints
        );
    }

    /**
     * Maps multiple Neo4j records to a Nodes collection
     */
    public function mapResultToNodes(
        array|SummarizedResult $result,
        WorkspaceName $workspaceName,
        DimensionSpacePoint $dimensionSpacePoint,
        VisibilityConstraints $visibilityConstraints
    ): Nodes {
        $nodes = [];
        foreach ($result as $record) {
            $nodes[] = $this->mapResultToNode(
                $record,
                $workspaceName,
                $dimensionSpacePoint,
                $visibilityConstraints
            );
        }

        return Nodes::fromArray($nodes);
    }

    public function mapResultToReference(
        \Laudis\Neo4j\Types\Node $result,
        WorkspaceName $workspaceName,
        DimensionSpacePoint $dimensionSpacePoint,
        VisibilityConstraints $visibilityConstraints,
        Relationship $relationship
    ): ?Reference {
        $properties = null;
        try {
            $properties = $relationship->getProperties()->hasKey('properties') ? $relationship->getProperties()->get('properties') : null;
        } catch (\Exception) {}
        return new Reference(
            $this->mapResultToNode(
                $result,
                $workspaceName,
                $dimensionSpacePoint,
                $visibilityConstraints
            ),
            ReferenceName::fromString($relationship->getProperty('referenceName')),
            $properties ? new PropertyCollection(
                SerializedPropertyValues::fromJsonString($properties ?: '{}'),
                $this->propertyConverter,
            ) : null,
        );
    }
    /**
     * Creates a PropertyCollection from a JSON string
     */
    private function createPropertyCollectionFromJsonString(string $jsonString): PropertyCollection
    {
        try {
            $values = SerializedPropertyValues::fromJsonString($jsonString);
        } catch (\Exception $e) {
            throw new \InvalidArgumentException(
                sprintf('Failed to parse properties JSON: %s', $e->getMessage()),
                1750173736,
                $e
            );
        }
        return new PropertyCollection(
            SerializedPropertyValues::fromJsonString($jsonString),
            $this->propertyConverter
        );
    }

    /**
     * Creates Timestamps from a Neo4j record
     */
    private function createTimestampsFromRecord(CypherMap $record): Timestamps
    {
        return Timestamps::create(
            $this->parseDateTimeString($record->get('created')),
            $this->parseDateTimeString($record->get('originalCreated')),
            ($record->hasKey('lastModified') && !empty($record->get('lastModified'))) ? $this->parseDateTimeString($record->get('lastModified')) : null,
            $record->hasKey('originalLastModified') && !empty($record->get('originalLastModified')) ? $this->parseDateTimeString($record->get('originalLastModified')) : null,
        );
    }

    /**
     * Parses a datetime string from Neo4j
     */
    private function parseDateTimeString(string $string): \DateTimeImmutable
    {
        // Neo4j stores datetime as ISO format, try to parse it
        $result = \DateTimeImmutable::createFromFormat(\DateTimeInterface::ATOM, $string);
        if ($result === false) {
            // Fallback to other common formats
            $result = \DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $string);
        }
        if ($result === false) {
            throw new \RuntimeException(sprintf('Failed to parse "%s" into a valid DateTime', $string), 1678902055);
        }
        return $result;
    }

    /**
     * Resolves an OriginDimensionSpacePoint from its hash
     */
    private function resolveDimensionSpacePointFromHash(?string $hash): OriginDimensionSpacePoint
    {
        return $this->dimensionSpacePointsRepository->getOriginDimensionSpacePointByHash($hash);
    }

    /**
     * Resolves a DimensionSpacePoint from its hash
     */
    private function resolveCoveredDimensionSpacePointFromHash(?string $hash): DimensionSpacePoint
    {
        return $this->dimensionSpacePointsRepository->getOriginDimensionSpacePointByHash($hash)->toDimensionSpacePoint();
    }
}
