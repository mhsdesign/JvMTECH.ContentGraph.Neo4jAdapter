<?php
declare(strict_types=1);
namespace JvMTECH\ContentGraph\Neo4jAdapter;

use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jContentGraph;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\Neo4jDimensionSpacePointsRepository;
use JvMTECH\ContentGraph\Neo4jAdapter\Domain\Repository\NodeFactory;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Types\CypherMap;
use Neos\ContentRepository\Core\NodeType\NodeTypeManager;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphInterface;
use Neos\ContentRepository\Core\Projection\ContentGraph\ContentGraphReadModelInterface;
use Neos\ContentRepository\Core\SharedModel\ContentRepository\ContentRepositoryId;
use Neos\ContentRepository\Core\SharedModel\Exception\WorkspaceDoesNotExist;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStream;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\ContentRepository\Core\SharedModel\Workspace\Workspace;
use Neos\ContentRepository\Core\SharedModel\Workspace\WorkspaceName;
use Neos\ContentRepository\Core\SharedModel\Workspace\Workspaces;
use Neos\ContentRepository\Core\SharedModel\Workspace\WorkspaceStatus;
use Neos\EventStore\Model\Event\Version;

class Neo4jContentGraphReadModelAdapter implements ContentGraphReadModelInterface
{
    public function __construct(
        private readonly ClientInterface $client,
        private readonly ContentRepositoryId $contentRepositoryId,
        private readonly NodeFactory $nodeFactory,
        private readonly Neo4jDimensionSpacePointsRepository $dimensionSpacePointsRepository,
        private readonly NodeTypeManager $nodeTypeManager,
    )
    {
    }

    public function getContentGraph(WorkspaceName $workspaceName): ContentGraphInterface
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (:Workspace {name: $workspaceName})-[:CONTENT_STREAM]->(contentStream:ContentStream) RETURN contentStream.contentStreamId AS contentStreamId',
                ['workspaceName' => $workspaceName->value]
            )
        );
        if ($result->isEmpty()) {
            throw WorkspaceDoesNotExist::butWasSupposedTo($workspaceName);
            throw new \RuntimeException(sprintf('No content stream found for workspace "%s".', $workspaceName->value), 1750171756);
        }
        $currentContentStreamId = ContentStreamId::fromString($result->getAsCypherMap(0)->getAsString('contentStreamId'));
        return new Neo4jContentGraph(
            $this->contentRepositoryId,
            $workspaceName,
            $this->client,
            $currentContentStreamId,
            $this->nodeFactory,
            $this->dimensionSpacePointsRepository,
            $this->nodeTypeManager,
        );
    }

    public function findContentStreamById(ContentStreamId $contentStreamId): ?ContentStream
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (contentStream:ContentStream {contentStreamId: $contentStreamId})
                OPTIONAL MATCH (contentStream)-[:SOURCE_CONTENT_STREAM]->(sourceContentStream)
                RETURN contentStream, sourceContentStream',
                ['contentStreamId' => $contentStreamId->value]
            )
        );
        if (!$result->hasKey(0) || !$result->getAsCypherMap(0)->hasKey('contentStream')) {
            return null;
        }
        $properties = $result->getAsCypherMap(0)->getAsNode('contentStream')->getProperties();
        $sourceContentStreamProperties = null;
        if ($result->getAsCypherMap(0)->hasKey('sourceContentStream') && !empty($result->getAsCypherMap(0)->get('sourceContentStream'))) {
            $sourceContentStreamProperties = $result->getAsCypherMap(0)->getAsNode('sourceContentStream')->getProperties();
        }
        return ContentStream::create(
            ContentStreamId::fromString($properties['contentStreamId']),
            $sourceContentStreamProperties ? ($sourceContentStreamProperties['contentStreamId'] ? ContentStreamId::fromString($sourceContentStreamProperties['contentStreamId']) : null) : null,
            Version::fromInteger($properties['version']),
            (bool)$properties['closed']
        );
    }

    public function countNodes(): int
    {
        $result = $this->client->runStatement(
            Statement::create('MATCH (n:Node) RETURN count(n) AS nodeCount')
        );
        if ($result->isEmpty()) {
            return 0;
        }
        return $result->getAsCypherMap(0)->getAsInt('nodeCount');
    }

    public function findWorkspaceByName(WorkspaceName $workspaceName): ?Workspace
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (workspace:Workspace {name: $workspaceName})
                OPTIONAL MATCH (workspace)-[:BASE_WORKSPACE]->(baseWorkspace:Workspace)
                MATCH (workspace)-[:CONTENT_STREAM]->(contentStream:ContentStream)
                RETURN workspace, baseWorkspace, contentStream',
                ['workspaceName' => $workspaceName->value]
            )
        );

        if (!$result->hasKey(0)) {
            return null;
        }
        $entry = $result->getAsCypherMap(0);
        $properties = $entry->getAsNode('workspace')->getProperties();
        $baseWorkspaceName = $entry->hasKey('baseWorkspace') && $entry->get('baseWorkspace') !== null ? $entry->getAsNode('baseWorkspace')->getProperty('name') : null;
        $contentStream = $entry->getAsNode('contentStream');

        return Workspace::create(
            WorkspaceName::fromString($properties['name']),
            $baseWorkspaceName ? WorkspaceName::fromString($baseWorkspaceName) : null,
            ContentStreamId::fromString($contentStream->getProperty('contentStreamId')),
            $contentStream->getProperty('hasChanges') === 0 || $baseWorkspaceName !== null ?
                WorkspaceStatus::UP_TO_DATE :
                WorkspaceStatus::OUTDATED,
            $contentStream->getProperty('hasChanges') !== 0 && $baseWorkspaceName !== null,
        );
    }

    public function findWorkspaces(): Workspaces
    {
        $result = $this->client->runStatement(
            Statement::create(
                'MATCH (workspace:Workspace)-[:CONTENT_STREAM]->(contentStream:ContentStream)
                OPTIONAL MATCH (workspace)-[:BASE_WORKSPACE]->(baseWorkspace:Workspace)
                RETURN workspace, baseWorkspace, contentStream',
            )
        );
        return Workspaces::fromArray(
            $result->map(function(CypherMap $entry) {
                $properties = $entry->getAsNode('workspace')->getProperties();
                $baseWorkspaceName = $entry->hasKey('baseWorkspace') && $entry->get('baseWorkspace') !== null ? $entry->getAsNode('baseWorkspace')->getProperty('name') : null;
                $contentStream = $entry->getAsNode('contentStream');

                return Workspace::create(
                    WorkspaceName::fromString($properties['name']),
                    $baseWorkspaceName ? WorkspaceName::fromString($baseWorkspaceName) : null,
                    ContentStreamId::fromString($contentStream->getProperty('contentStreamId')),
                    $contentStream->getProperty('hasChanges') === 0 || $baseWorkspaceName !== null ?
                        WorkspaceStatus::UP_TO_DATE :
                        WorkspaceStatus::OUTDATED,
                    $contentStream->getProperty('hasChanges') !== 0 && $baseWorkspaceName !== null,
                );
            })->toArray()
        );
    }
}
