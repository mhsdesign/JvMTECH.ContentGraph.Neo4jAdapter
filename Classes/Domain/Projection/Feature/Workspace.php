<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Projection\Feature;

use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Databags\Statement;
use Neos\ContentRepository\Core\SharedModel\Workspace\ContentStreamId;
use Neos\ContentRepository\Core\SharedModel\Workspace\WorkspaceName;

trait Workspace
{
    private readonly ClientInterface $client;

    private function createWorkspace(WorkspaceName $workspaceName, ?WorkspaceName $baseWorkspaceName, ContentStreamId $contentStreamId): void
    {
        if (!empty($baseWorkspaceName)) {
            $this->client->runStatement(
                Statement::create(
                    'CREATE (workspace:Workspace {name: $workspaceName})
                    WITH workspace
                    MATCH (c:ContentStream {contentStreamId: $contentStreamId})
                    MATCH (bw:Workspace {name: $baseWorkspaceName})
                    MERGE (workspace)-[:CONTENT_STREAM]->(c)
                    MERGE (workspace)-[:BASE_WORKSPACE]->(bw)
                    ',
                    [
                        'workspaceName' => $workspaceName->value,
                        'contentStreamId' => $contentStreamId->value,
                        'baseWorkspaceName' => $baseWorkspaceName->value,
                    ]
                )
            );
        } else {
            $this->client->runStatement(
                Statement::create(
                    'CREATE (workspace:Workspace {name: $workspaceName})
                    WITH workspace
                    MATCH (c:ContentStream {contentStreamId: $contentStreamId})
                    MERGE (workspace)-[:CONTENT_STREAM]->(c)
                    ',
                    [
                        'workspaceName' => $workspaceName->value,
                        'contentStreamId' => $contentStreamId->value,
                    ]
                )
            );
        }
    }

    private function removeWorkspace(WorkspaceName $workspaceName): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (workspace:Workspace {name: $workspaceName})
                 DETACH DELETE workspace',
                ['workspaceName' => $workspaceName->value]
            )
        );
    }
    private function updateBaseWorkspace(
        WorkspaceName $workspaceName,
        WorkspaceName $baseWorkspaceName,
        ContentStreamId $newContentStreamId
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (workspace:Workspace {name: $workspaceName})
                 MATCH (workspace)-[bwRel:BASE_WORKSPACE]->()
                 MATCH (workspace-[csRel:CONTENT_STREAM]->()
                 DELETE bwRel, csRel
                 WITH workspace
                 MATCH (newBw:Workspace {name: $baseWorkspaceName})
                 MATCH (newCs:ContentStream {contentStreamId: $newContentStreamId})
                 MERGE (workspace)-[:BASE_WORKSPACE]->(newBw)
                MERGE (workspace)-[:CONTENT_STREAM]->(newCs)',
                [
                    'workspaceName' => $workspaceName->value,
                    'baseWorkspaceName' => $baseWorkspaceName->value,
                    'newContentStreamId' => $newContentStreamId->value,
                ]
            )
        );
    }

    private function updateWorkspaceContentStreamId(
        WorkspaceName $workspaceName,
        ContentStreamId $newContentStreamId
    ): void
    {
        $this->client->runStatement(
            Statement::create(
                'MATCH (workspace:Workspace {name: $workspaceName})
                 MATCH (workspace)-[csRel:CONTENT_STREAM]->()
                 DELETE csRel
                 WITH workspace
                 MATCH (newCs:ContentStream {contentStreamId: $newContentStreamId})
                 MERGE (workspace)-[:CONTENT_STREAM]->(newCs)',
                [
                    'workspaceName' => $workspaceName->value,
                    'newContentStreamId' => $newContentStreamId->value,
                ]
            )
        );
    }
}
