<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\EventStore\Model\Event\Version;

trait ContentStream
{
    private readonly ClientInterface $client;
    private function createContentStream(ContentStreamId $contentStreamId, ?ContentStreamId $sourceContentStreamId = null, ?Version $sourceVersion = null): SummarizedResult
    {
        return $this->client->runStatement(
            Statement::create(
                'CREATE (contentStream:ContentStream {contentStreamId: $contentStreamId, version: 0, closed: 0, hasChanges: 0})
                WITH contentStream
                WHERE $sourceContentStreamId IS NOT NULL
                OPTIONAL MATCH (sourceContentStream:ContentStream {contentStreamId: $sourceContentStreamId})
                MERGE (contentStream)-[:SOURCE_CONTENT_STREAM {sourceContentStreamVersion: sourceContentStream.version}]->(sourceContentStream)
                RETURN contentStream',
                [
                    'contentStreamId' => $contentStreamId->value,
                    'sourceContentStreamId' => $sourceContentStreamId?->value,
                    'sourceVersion' => $sourceVersion?->value
                ]
            )
        );
    }

    private function closeContentStream(ContentStreamId $contentStreamId): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (contentStream:ContentStream {contentStreamId: $contentStreamId}) SET contentStream.closed = 1',
                ['contentStreamId' => $contentStreamId->value]
            )
        );
    }

    private function reopenContentStream(ContentStreamId $contentStreamId): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (contentStream:ContentStream {contentStreamId: $contentStreamId}) SET contentStream.closed = 0',
                ['contentStreamId' => $contentStreamId->value]
            )
        );
    }

    private function removeContentStream(ContentStreamId $contentStreamId): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (contentStream:ContentStream {contentStreamId: $contentStreamId}) DETACH DELETE contentStream',
                ['contentStreamId' => $contentStreamId->value]
            )
        );
    }

    private function updateContentStreamVersion(ContentStreamId $contentStreamId, Version $version, bool $markAsDirty): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (contentStream:ContentStream {contentStreamId: $contentStreamId}) SET contentStream.version = $version, contentStream.hasChanges = CASE WHEN contentStream.hasChanges = 1 THEN 1 ELSE $hasChanges END',
                [
                    'contentStreamId' => $contentStreamId->value,
                    'version' => $version->value,
                    'hasChanges' => $markAsDirty ? 1 : 0
                ]
            )
        );
    }
}
