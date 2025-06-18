package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresSelect.SelectType;


/**
 * Represents a Common Table Expression (CTE) in PostgreSQL.
 * A CTE provides a way to write auxiliary statements for use in a larger query.
 */
public class PostgresCTE implements PostgresExpression {
    private final String name;
    private final List<String> columnAliases;
    private final PostgresSelect query;
    private final List<PostgresColumn> columns;

    public PostgresCTE(String name, List<String> columnAliases, PostgresSelect query, List<PostgresColumn> columns) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("CTE name cannot be null or empty");
        }
        if (query == null) {
            throw new IllegalArgumentException("CTE query cannot be null");
        }
        if (query.getSelectOption() == SelectType.DISTINCT && query.getForClause() != null) {
            throw new IllegalArgumentException("FOR UPDATE is not allowed with DISTINCT clause");
        }
        this.name = name;
        this.columnAliases = columnAliases != null ? new ArrayList<>(columnAliases) : new ArrayList<>();
        this.query = query;
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnAliases() {
        return new ArrayList<>(columnAliases);
    }

    public PostgresSelect getQuery() {
        return query;
    }

    public List<PostgresColumn> getColumns() {
        return new ArrayList<>(columns);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return null;
    }

    /**
     * Generates the SQL string representation of this CTE.
     * Format: cte_name [(column_alias, ...)] AS ( query )
     */
    public String asString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        
        // Add column aliases if present
        if (!columnAliases.isEmpty()) {
            sb.append("(");
            sb.append(String.join(", ", columnAliases));
            sb.append(")");
        }
        
        sb.append(" AS (");
        sb.append(query.asString());
        sb.append(")");
        
        return sb.toString();
    }

    /**
     * Creates a builder for PostgresCTE.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for PostgresCTE to facilitate the construction of CTE instances.
     */
    public static class Builder {
        private String name;
        private List<String> columnAliases = new ArrayList<>();
        private PostgresSelect query;
        private List<PostgresColumn> columns = new ArrayList<>();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder columnAliases(List<String> columnAliases) {
            this.columnAliases = columnAliases;
            return this;
        }

        public Builder query(PostgresSelect query) {
            this.query = query;
            return this;
        }

        public Builder columns(List<PostgresColumn> columns) {
            this.columns = columns;
            return this;
        }

        public PostgresCTE build() {
            return new PostgresCTE(name, columnAliases, query, columns);
        }
    }
}
