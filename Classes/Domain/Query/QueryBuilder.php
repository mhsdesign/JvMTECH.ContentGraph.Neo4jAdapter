<?php
declare(strict_types=1);

namespace JvMTECH\ContentGraph\Neo4jAdapter\Domain\Query;

use Laudis\Neo4j\Databags\Statement;

/**
 * Fluent query builder for Neo4j Cypher queries with ordered statement support
 *
 * @internal
 */
class QueryBuilder
{
    private array $clauses = [];
    private array $parameters = [];

    public function match(string $pattern): self
    {
        $this->addClause('MATCH', $pattern);
        return $this;
    }

    public function optionalMatch(string $pattern): self
    {
        $this->addClause('OPTIONAL MATCH', $pattern);
        return $this;
    }

    public function where(string $condition): self
    {
        $this->addClause('WHERE', $condition);
        return $this;
    }

    public function with(string $expression): self
    {
        $this->addClause('WITH', $expression);
        return $this;
    }

    public function returns(string $expression): self
    {
        $this->addClause('RETURN', $expression);
        return $this;
    }

    public function returnDistinct(string $expression): self
    {
        $this->addClause('RETURN', 'DISTINCT ' . $expression);
        return $this;
    }

    public function orderBy(string $expression, string $direction = 'ASC'): self
    {
        $this->addClause('ORDER BY', $expression . ' ' . strtoupper($direction));
        return $this;
    }

    public function create(string $pattern): self
    {
        $this->addClause('CREATE', $pattern);
        return $this;
    }

    public function merge(string $pattern): self
    {
        $this->addClause('MERGE', $pattern);
        return $this;
    }

    public function delete(string $expression): self
    {
        $this->addClause('DELETE', $expression);
        return $this;
    }

    public function detachDelete(string $expression): self
    {
        $this->addClause('DETACH DELETE', $expression);
        return $this;
    }

    public function set(string $expression): self
    {
        $this->addClause('SET', $expression);
        return $this;
    }

    public function remove(string $expression): self
    {
        $this->addClause('REMOVE', $expression);
        return $this;
    }

    public function limit(int $limit): self
    {
        $this->addClause('LIMIT', (string)$limit);
        return $this;
    }

    public function skip(int $skip): self
    {
        $this->addClause('SKIP', (string)$skip);
        return $this;
    }

    public function foreach(string $expression): self
    {
        $this->addClause('FOREACH', $expression);
        return $this;
    }

    public function call(string $procedure): self
    {
        $this->addClause('CALL', $procedure);
        return $this;
    }

    public function yield(string $expression): self
    {
        $this->addClause('YIELD', $expression);
        return $this;
    }

    public function unwind(string $expression): self
    {
        $this->addClause('UNWIND', $expression);
        return $this;
    }

    public function union(bool $all = false): self
    {
        $clause = $all ? 'UNION ALL' : 'UNION';
        $this->addClause($clause, '');
        return $this;
    }

    public function rawClause(string $clause, string $expression = ''): self
    {
        $this->addClause($clause, $expression);
        return $this;
    }

    private function addClause(string $type, string $expression): void
    {
        $this->clauses[] = [
            'type' => $type,
            'expression' => $expression,
        ];
    }

    public function withParameters(array $parameters): self
    {
        $this->parameters = array_merge($this->parameters, $parameters);
        return $this;
    }

    public function withParameter(string $key, mixed $value): self
    {
        $this->parameters[$key] = $value;
        return $this;
    }

    private function buildCypher(): string
    {
        $cypher = [];
        $lastClause = null;

        foreach ($this->clauses as $clause) {
            $type = $clause['type'];
            $expression = $clause['expression'];

            if ($expression === '') {
                $cypher[] = $type;
            } else {
                if ($type === 'WHERE' && $lastClause['type'] === 'WHERE') {
                    $type = 'AND';
                }
                if ($type === 'ORDER BY' && $lastClause['type'] === 'ORDER BY') {
                    $type = ',';
                }
                $cypher[] = $type . ' ' . $expression;
            }
            $lastClause = $clause;
        }

        return implode("\n", array_filter($cypher));
    }

    public function getParameters(): array
    {
        return $this->parameters;
    }

    public function build(): Statement
    {
        return Statement::create($this->buildCypher(), $this->getParameters());
    }

    public function getClauses(): array
    {
        return $this->clauses;
    }
}
