<?php

declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter;

use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\ProjectionIntegrityViolationDetector;
use Neos\ContentRepository\Core\Factory\ContentRepositoryServiceFactoryDependencies;
use Neos\ContentRepository\Core\Projection\ContentGraph\ProjectionIntegrityViolationDetectionRunner;
use Neos\ContentRepository\Core\Projection\ContentGraph\ProjectionIntegrityViolationDetectionRunnerFactoryInterface;

/**
 * @internal
 */
class Neos4jProjectionIntegrityViolationDetectionRunnerFactory implements ProjectionIntegrityViolationDetectionRunnerFactoryInterface
{
    public function build(
        ContentRepositoryServiceFactoryDependencies $serviceFactoryDependencies
    ): ProjectionIntegrityViolationDetectionRunner {
        return new ProjectionIntegrityViolationDetectionRunner(
            new ProjectionIntegrityViolationDetector()
        );
    }
}
