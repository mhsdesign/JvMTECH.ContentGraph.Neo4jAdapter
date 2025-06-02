<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter;

use Neos\Flow\Annotations as Flow;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jDimensionSpacePointsRepository;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jProjectionContentGraph;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\NodeFactory as Neo4jNodeFactory;
use Laudis\Neo4j\Authentication\Authenticate;
use Laudis\Neo4j\ClientBuilder;
use Neos\ContentRepository\Core\Factory\SubscriberFactoryDependencies;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphProjectionFactoryInterface;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphProjectionInterface;

class Neo4jContentGraphProjectionFactory implements ContentGraphProjectionFactoryInterface
{
    #[Flow\InjectConfiguration(path: 'Connection')]
    protected array $config = [];
    public function build(
        SubscriberFactoryDependencies $projectionFactoryDependencies
    ): ContentGraphProjectionInterface {
        $client = ClientBuilder::create()
            ->withDriver('driver', $this->config['host'], Authenticate::basic($this->config['username'], $this->config['password']))
            ->build();

        $neo4jDimensionSpacePointsRepository = new Neo4jDimensionSpacePointsRepository($client);
        $neo4jNodeFactory = new Neo4jNodeFactory(
            $projectionFactoryDependencies->contentRepositoryId,
            $neo4jDimensionSpacePointsRepository,
            $projectionFactoryDependencies->getPropertyConverter(),
        );

        $projectionState = new Neo4jContentGraphReadModelAdapter(
            $client,
            $projectionFactoryDependencies->contentRepositoryId,
            $neo4jNodeFactory,
            $neo4jDimensionSpacePointsRepository,
            $projectionFactoryDependencies->nodeTypeManager,
        );

        return new Neo4jContentGraphProjection(
            $client,
            $projectionState,
            $neo4jDimensionSpacePointsRepository,
            new Neo4jProjectionContentGraph($client),
        );
    }
}
