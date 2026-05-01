<?php

declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection;

use Neos\ContentRepository\Core\Projection\ContentGraph\ProjectionIntegrityViolationDetectorInterface;
use Neos\Error\Messages\Result;

/**
 * The Neo4j database backend implementation for projection invariant checks
 *
 * TODO currently not implemented. Most invariants are already handled by the graph database. Though some specific cases must be implemented.
 *
 * @internal
 */
final class ProjectionIntegrityViolationDetector implements ProjectionIntegrityViolationDetectorInterface
{
    public function hierarchyIntegrityIsProvided(): Result
    {
        return new Result();
    }

    public function siblingsAreDistinctlySorted(): Result
    {
        return new Result();
    }

    public function tetheredNodesAreNamed(): Result
    {
        return new Result();
    }

    public function subtreeTagsAreInherited(): Result
    {
        return new Result();
    }

    public function referenceIntegrityIsProvided(): Result
    {
        return new Result();
    }

    public function referencesAreDistinctlySorted(): Result
    {
        return new Result();
    }

    public function allNodesAreConnectedToARootNodePerSubgraph(): Result
    {
        return new Result();
    }

    public function allNodesHaveAtMostOneParentPerSubgraph(): Result
    {
        return new Result();
    }

    public function nonRootNodesHaveParents(): Result
    {
        return new Result();
    }

    public function nodeAggregateIdsAreUniquePerSubgraph(): Result
    {
        return new Result();
    }

    public function nodeAggregatesAreConsistentlyTypedPerContentStream(): Result
    {
        return new Result();
    }

    public function nodeAggregatesAreConsistentlyClassifiedPerContentStream(): Result
    {
        return new Result();
    }

    public function childNodeCoverageIsASubsetOfParentNodeCoverage(): Result
    {
        return new Result();
    }

    public function allNodesCoverTheirOrigin(): Result
    {
        return new Result();
    }
}
