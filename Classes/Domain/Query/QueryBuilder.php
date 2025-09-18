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

    public function debug(?string $title = null): static
    {
        $text = $this->build()->getText();
        foreach ($this->getParameters() as $parameter => $value) {
            $replacement = is_string($value) ? "'" . $value . "'" : (is_array($value) ? '["' . implode('", "', $value) . '"]' : (is_null($value) ? 'NULL' : (is_bool($value) ? ($value ? 'TRUE' : 'FALSE') : $value)));
            $text = str_replace('$' . $parameter, (string)$replacement, $text);
        }
        \Neos\Flow\var_dump($text, $title);
        return $this;
    }
    public function match(string $pattern): static
    {
        $this->addClause('MATCH', $pattern);
        return $this;
    }

    public function optionalMatch(string $pattern): static
    {
        $this->addClause('OPTIONAL MATCH', $pattern);
        return $this;
    }

    public function where(string $condition): static
    {
        if (!empty($condition)) {
            $this->addClause('WHERE', $condition);
        }
        return $this;
    }

    public function with(string $expression): static
    {
        $this->addClause('WITH', $expression);
        return $this;
    }

    public function returns(string $expression): static
    {
        $this->addClause('RETURN', $expression);
        return $this;
    }

    public function returnDistinct(string $expression): static
    {
        $this->addClause('RETURN', 'DISTINCT ' . $expression);
        return $this;
    }

    public function orderBy(string $expression, string $direction = 'ASC'): static
    {
        $this->addClause('ORDER BY', $expression . ' ' . strtoupper($direction));
        return $this;
    }

    public function create(string $pattern): static
    {
        $this->addClause('CREATE', $pattern);
        return $this;
    }

    public function merge(string $pattern): static
    {
        $this->addClause('MERGE', $pattern);
        return $this;
    }

    public function delete(string $expression): static
    {
        $this->addClause('DELETE', $expression);
        return $this;
    }

    public function detachDelete(string $expression): static
    {
        $this->addClause('DETACH DELETE', $expression);
        return $this;
    }

    public function set(string $expression): static
    {
        $this->addClause('SET', $expression);
        return $this;
    }

    public function remove(string $expression): static
    {
        $this->addClause('REMOVE', $expression);
        return $this;
    }

    public function limit(int $limit): static
    {
        $this->addClause('LIMIT', (string)$limit);
        return $this;
    }

    public function skip(int $skip): static
    {
        $this->addClause('SKIP', (string)$skip);
        return $this;
    }

    public function foreach(string $expression): static
    {
        $this->addClause('FOREACH', $expression);
        return $this;
    }

    public function call(string $procedure): static
    {
        $this->addClause('CALL', $procedure);
        return $this;
    }

    public function yield(string $expression): static
    {
        $this->addClause('YIELD', $expression);
        return $this;
    }

    public function unwind(string $expression): static
    {
        $this->addClause('UNWIND', $expression);
        return $this;
    }

    public function union(bool $all = false): static
    {
        $clause = $all ? 'UNION ALL' : 'UNION';
        $this->addClause($clause, '');
        return $this;
    }

    public function rawClause(string $clause, string $expression = ''): static
    {
        $this->addClause($clause, $expression);
        return $this;
    }

    /**
    * @param callable(static): static $subClauses
    */
    public function rawClauseBuilder(callable $subClauses): static
    {
        $statement = $subClauses(new static())->build();
        $text = $statement->getText();
        foreach ($statement->getParameters() as $parameter => $value) {
            $replacement = is_string($value) ? "'" . $value . "'" : (is_array($value) ? '["' . implode('", "', $value) . '"]' : (is_null($value) ? 'NULL' : (is_bool($value) ? ($value ? 'TRUE' : 'FALSE') : $value)));
            $text = str_replace('$' . $parameter, $replacement, $text);
        }
        return $this->rawClause($text);
    }

    private function addClause(string $type, string $expression): void
    {
        $this->clauses[] = [
            'type' => $type,
            'expression' => $expression,
        ];
    }

    public function withParameters(array $parameters): static
    {
        $this->parameters = array_merge($this->parameters, $parameters);
        return $this;
    }

    public function withParameter(string $key, mixed $value): static
    {
        $this->parameters[$key] = $value;
        return $this;
    }

    private function buildCypher(): string
    {
        $cypher = [];
        $lastClause = [];
        $appendClauses = [];

        foreach ($this->clauses as $clause) {
            $type = $clause['type'];
            $expression = $clause['expression'];

            if ($expression === '') {
                $cypher[] = $type;
            } else {
                if (array_key_exists('type', $lastClause)) {
                    if ($type === 'WHERE' && $lastClause['type'] === 'WHERE') {
                        $type = 'AND';
                    }
                    if ($type === 'ORDER BY' && $lastClause['type'] === 'ORDER BY') {
                        $type = ',';
                    }
                }
                if ($type === 'RETURN') {
                    $appendClauses[] = $clause;
                    continue;
                }
                $cypher[] = $type . ' ' . $expression;
            }
            $lastClause = $clause;
        }
        foreach ($appendClauses as $appendClause) {
            $cypher[] = $appendClause['type'] . ' ' . $appendClause['expression'];
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

    /**
     * @param callable(static): static $subClauses
     */
    public function whereAll(callable $subClauses, string $groupAlias = 'rels'): static
    {
        $statement = $subClauses(new static())->build();
        $this
            ->where(sprintf('all(rel IN %s %s)', $groupAlias, $statement->getText()))
            ->withParameters($statement->getParameters());
        return $this;
    }
}
