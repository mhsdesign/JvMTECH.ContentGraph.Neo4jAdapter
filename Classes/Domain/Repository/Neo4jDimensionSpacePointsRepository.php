<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository;

use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Neos\ContentRepository\Core\DimensionSpace\AbstractDimensionSpacePoint;
use Neos\ContentRepository\Core\DimensionSpace\OriginDimensionSpacePoint;

class Neo4jDimensionSpacePointsRepository
{
    /** @var array{string, string} $dimensionSpacePointsRuntimeCache */
    private array $dimensionSpacePointsRuntimeCache = [];

    public function __construct(
        private readonly ClientInterface $client,
    ) {
    }

    public function insertDimensionSpacePoint(AbstractDimensionSpacePoint $dimensionSpacePoint): void
    {
        if ($this->getCoordinatesByHashFromRuntimeCache($dimensionSpacePoint->hash) !== null) {
            return;
        }

        $this->dimensionSpacePointsRuntimeCache[$dimensionSpacePoint->hash] = $dimensionSpacePoint->toJson();
        $this->writeDimensionSpacePoint($dimensionSpacePoint->hash, $dimensionSpacePoint->toJson());
    }

    public function getOriginDimensionSpacePointByHash(string $hash): OriginDimensionSpacePoint
    {
        $coordinates = $this->getCoordinatesByHashFromRuntimeCache($hash);
        if ($coordinates === null) {
            $this->fillRuntimeCacheFromDatabase();
            $coordinates = $this->getCoordinatesByHashFromRuntimeCache($hash);
        }

        if ($coordinates === null) {
            throw new \RuntimeException(sprintf('A DimensionSpacePoint record with the given hash "%s" was not found in the projection, cannot determine coordinates.', $hash), 1749125277);
        }

        return OriginDimensionSpacePoint::fromJsonString($coordinates);
    }

    private function writeDimensionSpacePoint(string $hash, string $dimensionSpacePointJson): void
    {
        $this->client->runStatement(
            Statement::create(
                'MERGE (dsp:DimensionSpacePoint {hash: $hash}) SET dsp.coordinates = $coordinates',
                [
                    'hash' => $hash,
                    'coordinates' => $dimensionSpacePointJson,
                ],
            )
        );
    }

    private function getCoordinatesByHashFromRuntimeCache(string $hash): ?string
    {
        return $this->dimensionSpacePointsRuntimeCache[$hash] ?? null;
    }

    private function fillRuntimeCacheFromDatabase(): void
    {
        /** @var SummarizedResult $allDimensionSpacePoints */
        $allDimensionSpacePoints = $this->client->runStatement(
            Statement::create('MATCH (dsp:DimensionSpacePoint) RETURN dsp.hash AS hash, dsp.coordinates AS coordinates')
        );
        /** @var CypherMap $dimensionSpacePointResult */
        foreach ($allDimensionSpacePoints as $dimensionSpacePointResult) {
            $this->dimensionSpacePointsRuntimeCache[$dimensionSpacePointResult->get('hash')] = $dimensionSpacePointResult->get('coordinates');
        }
    }
}
