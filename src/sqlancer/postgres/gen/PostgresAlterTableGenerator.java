package sqlancer.postgres.gen;

import java.util.List;
import java.util.ArrayList;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public class PostgresAlterTableGenerator {

    private PostgresTable randomTable;
    private Randomly r;
    private static PostgresColumn randomColumn;
    private boolean generateOnlyKnown;
    private List<String> opClasses;
    private PostgresGlobalState globalState;

    protected enum Action {
        // ALTER_TABLE_ADD_COLUMN, // [ COLUMN ] column data_type [ COLLATE collation ] [
        // column_constraint [ ... ] ]
        ALTER_TABLE_DROP_COLUMN, // DROP [ COLUMN ] [ IF EXISTS ] column [ RESTRICT | CASCADE ]
        ALTER_COLUMN_TYPE, // ALTER [ COLUMN ] column [ SET DATA ] TYPE data_type [ COLLATE collation ] [
                           // USING expression ]
        ALTER_COLUMN_SET_DROP_DEFAULT, // ALTER [ COLUMN ] column SET DEFAULT expression and ALTER [ COLUMN ] column
                                       // DROP DEFAULT
        ALTER_COLUMN_SET_DROP_NULL, // ALTER [ COLUMN ] column { SET | DROP } NOT NULL
        ALTER_COLUMN_SET_STATISTICS, // ALTER [ COLUMN ] column SET STATISTICS integer
        ALTER_COLUMN_SET_ATTRIBUTE_OPTION, // ALTER [ COLUMN ] column SET ( attribute_option = value [, ... ] )
        ALTER_COLUMN_RESET_ATTRIBUTE_OPTION, // ALTER [ COLUMN ] column RESET ( attribute_option [, ... ] )
        ALTER_COLUMN_SET_STORAGE, // ALTER [ COLUMN ] column SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
        ADD_TABLE_CONSTRAINT, // ADD table_constraint [ NOT VALID ]
        ADD_TABLE_CONSTRAINT_USING_INDEX, // ADD table_constraint_using_index
        VALIDATE_CONSTRAINT, // VALIDATE CONSTRAINT constraint_name
        DISABLE_ROW_LEVEL_SECURITY, // DISABLE ROW LEVEL SECURITY
        ENABLE_ROW_LEVEL_SECURITY, // ENABLE ROW LEVEL SECURITY
        FORCE_ROW_LEVEL_SECURITY, // FORCE ROW LEVEL SECURITY
        NO_FORCE_ROW_LEVEL_SECURITY, // NO FORCE ROW LEVEL SECURITY
        CLUSTER_ON, // CLUSTER ON index_name
        SET_WITHOUT_CLUSTER, //
        SET_WITH_OIDS, //
        SET_WITHOUT_OIDS, //
        SET_LOGGED_UNLOGGED, //
        NOT_OF, //
        OWNER_TO, //
        REPLICA_IDENTITY,
        ALTER_COLUMN_SET_IDENTITY,
        ALTER_COLUMN_SET_GENERATED,
        ALTER_COLUMN_DROP_IDENTITY,
        ALTER_COLUMN_RESTART
    }

    public PostgresAlterTableGenerator(PostgresTable randomTable, PostgresGlobalState globalState,
            boolean generateOnlyKnown) {
        this.randomTable = randomTable;
        this.globalState = globalState;
        this.r = globalState.getRandomly();
        this.generateOnlyKnown = generateOnlyKnown;
        this.opClasses = globalState.getOpClasses();
    }

    public static SQLQueryAdapter create(PostgresTable randomTable, PostgresGlobalState globalState,
            boolean generateOnlyKnown) {
        return new PostgresAlterTableGenerator(randomTable, globalState, generateOnlyKnown).generate();
    }

    private enum Attribute {
        N_DISTINCT_INHERITED("n_distinct_inherited"), N_DISTINCT("n_distinct");

        private String val;

        Attribute(String val) {
            this.val = val;
        }
    };

    public List<Action> getActions(ExpectedErrors errors) {
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        PostgresCommon.addCommonTableErrors(errors);
        errors.add("cannot drop desired object(s) because other objects depend on them");
        errors.add("invalid input syntax for");
        errors.add("it has pending trigger events");
        errors.add("could not open relation");
        errors.add("functions in index expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not accept data type");
        errors.add("does not exist for access method");
        errors.add("could not find cast from");
        errors.add("does not exist"); // TODO: investigate
        errors.add("constraints on permanent tables may reference only permanent tables");
        List<Action> actions = new ArrayList<>();
        if (Randomly.getBoolean()) {
            actions = Randomly.nonEmptySubset(Action.values());
        } else {
            // make it more likely that the ALTER TABLE succeeds
            actions = Randomly.subset(Randomly.smallNumber(), Action.values());
        }
        if (randomTable.getColumns().size() == 1) {
            actions.remove(Action.ALTER_TABLE_DROP_COLUMN);
        }
        
        // Add identity-related actions only for INT columns
        PostgresColumn randomColumn = randomTable.getRandomColumn();
        if (randomColumn.getType() == PostgresDataType.INT) {
            actions.add(Action.ALTER_COLUMN_SET_IDENTITY);
            actions.add(Action.ALTER_COLUMN_SET_GENERATED);
            actions.add(Action.ALTER_COLUMN_DROP_IDENTITY);
            actions.add(Action.ALTER_COLUMN_RESTART);
        }
        
        if (randomTable.getIndexes().isEmpty()) {
            actions.remove(Action.ADD_TABLE_CONSTRAINT_USING_INDEX);
            actions.remove(Action.CLUSTER_ON);
        }
        actions.remove(Action.SET_WITH_OIDS);
        if (!randomTable.hasIndexes()) {
            actions.remove(Action.ADD_TABLE_CONSTRAINT_USING_INDEX);
        }
        if (actions.isEmpty()) {
            throw new IgnoreMeException();
        }
        return actions;
    }

    public SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        List<Action> actions = getActions(errors);
        
        // Skip if no actions
        if (actions.isEmpty()) {
            throw new IgnoreMeException();
        }
        
        List<String> validStatements = new ArrayList<>();
        
        for (Action a : actions) {
            if (a == null) {
                continue;
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER TABLE ");
            if (Randomly.getBoolean()) {
                sb.append("ONLY ");
            }
            sb.append(randomTable.getName());
            sb.append(" ");
            
            int lengthBefore = sb.length();
            boolean actionGenerated = false;
            
            switch (a) {
                case ALTER_TABLE_DROP_COLUMN:
                    sb.append("DROP COLUMN ");
                    if (Randomly.getBoolean()) {
                        sb.append("IF EXISTS ");
                    }
                    sb.append(randomTable.getRandomColumn().getName());
                    if (Randomly.getBoolean()) {
                        sb.append(" ");
                        sb.append(Randomly.fromOptions("RESTRICT", "CASCADE"));
                    }
                    errors.add("because other objects depend on it");
                    errors.add("does not exist");
                    errors.add("cannot drop column");
                    errors.add("cannot drop inherited column");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_TYPE:
                    alterColumn(randomTable, sb);
                    if (Randomly.getBoolean()) {
                        sb.append("SET DATA ");
                    }
                    sb.append("TYPE ");
                    PostgresDataType randomType = PostgresDataType.getRandomType();
                    sb.append(randomType.toString());
                    errors.add("cannot be cast automatically to type");
                    errors.add("foreign key constraint");
                    errors.add("cannot alter type of a column used by a view or rule");
                    errors.add("cannot convert infinity to numeric");
                    errors.add("is duplicated");
                    errors.add("cannot be cast automatically");
                    errors.add("is an identity column");
                    errors.add("identity column type must be smallint, integer, or bigint");
                    errors.add("out of range");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_SET_DROP_DEFAULT:
                    alterColumn(randomTable, sb);
                    if (Randomly.getBoolean()) {
                        sb.append("SET DEFAULT ");
                        sb.append(globalState.getRandomly().getInteger());
                    } else {
                        sb.append("DROP DEFAULT");
                    }
                    errors.add("invalid input syntax");
                    errors.add("is a generated column");
                    errors.add("is an identity column");
                    errors.add("Use ALTER TABLE ... ALTER COLUMN ... DROP IDENTITY instead");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_SET_DROP_NULL:
                    alterColumn(randomTable, sb);
                    if (Randomly.getBoolean()) {
                        sb.append("SET NOT NULL");
                        errors.add("contains null values");
                    } else {
                        sb.append("DROP NOT NULL");
                        errors.add("is in a primary key");
                        errors.add("is an identity column");
                    }
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_SET_STATISTICS:
                    alterColumn(randomTable, sb);
                    sb.append("SET STATISTICS ");
                    sb.append(globalState.getRandomly().getInteger(-1, 10000));
                    errors.add("must be between");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_SET_ATTRIBUTE_OPTION:
                    alterColumn(randomTable, sb);
                    sb.append("SET (n_distinct=");
                    double nDistinct = globalState.getRandomly().getDouble() * 2 - 1; // Range: -1 to 1
                    sb.append(nDistinct);
                    sb.append(")");
                    errors.add("value out of bounds for option");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_RESET_ATTRIBUTE_OPTION:
                    alterColumn(randomTable, sb);
                    sb.append("RESET (n_distinct)");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_SET_STORAGE:
                    alterColumn(randomTable, sb);
                    sb.append("SET STORAGE ");
                    if (randomColumn != null) {
                        if (randomColumn.getType() == PostgresDataType.INT || 
                            randomColumn.getType().toString().contains("int") ||
                            randomColumn.getType().toString().contains("numeric")) {
                            sb.append("PLAIN");
                        } else {
                            sb.append(Randomly.fromOptions("PLAIN", "EXTERNAL", "EXTENDED", "MAIN"));
                        }
                    }
                    errors.add("can only have storage PLAIN");
                    actionGenerated = true;
                    break;
                    
                case ALTER_COLUMN_SET_IDENTITY:
                    // Skip identity operations entirely to avoid type conflicts
                    // Since we can't track type changes within the same statement
                    continue;
                    
                case ALTER_COLUMN_SET_GENERATED:
                    // Skip to avoid "is not an identity column" errors
                    continue;
                    
                case ALTER_COLUMN_DROP_IDENTITY:
                    if (randomColumn != null) {
                        alterColumn(randomTable, sb);
                        sb.append("DROP IDENTITY");
                        if (Randomly.getBoolean()) {
                            sb.append(" IF EXISTS");
                        }
                        actionGenerated = true;
                    }
                    errors.add("is not an identity column");
                    errors.add("column does not exist");
                    break;
                    
                case ALTER_COLUMN_RESTART:
                    if (randomColumn != null) {
                        alterColumn(randomTable, sb);
                        sb.append("RESTART");
                        if (Randomly.getBoolean()) {
                            sb.append(" WITH ");
                            sb.append(globalState.getRandomly().getInteger(2, 200));
                        }
                        actionGenerated = true;
                    }
                    errors.add("is not an identity column");
                    errors.add("must be greater than or equal to");
                    errors.add("cannot restart identity column");
                    errors.add("column does not exist");
                    break;
                    
                case ADD_TABLE_CONSTRAINT:
                    sb.append("ADD CONSTRAINT c");
                    sb.append(Math.abs(globalState.getRandomly().getInteger()));
                    sb.append(" CHECK (TRUE)");
                    errors.add("already exists");
                    errors.add("violates check constraint");
                    actionGenerated = true;
                    break;
                    
                case VALIDATE_CONSTRAINT:
                    sb.append("VALIDATE CONSTRAINT asdf");
                    errors.add("does not exist");
                    actionGenerated = true;
                    break;
                    
                case DISABLE_ROW_LEVEL_SECURITY:
                    sb.append("DISABLE ROW LEVEL SECURITY");
                    actionGenerated = true;
                    break;
                    
                case ENABLE_ROW_LEVEL_SECURITY:
                    sb.append("ENABLE ROW LEVEL SECURITY");
                    actionGenerated = true;
                    break;
                    
                case FORCE_ROW_LEVEL_SECURITY:
                    sb.append("FORCE ROW LEVEL SECURITY");
                    actionGenerated = true;
                    break;
                    
                case NO_FORCE_ROW_LEVEL_SECURITY:
                    sb.append("NO FORCE ROW LEVEL SECURITY");
                    actionGenerated = true;
                    break;
                    
                case SET_WITHOUT_CLUSTER:
                    sb.append("SET WITHOUT CLUSTER");
                    actionGenerated = true;
                    break;
                    
                case SET_WITHOUT_OIDS:
                    sb.append("SET WITHOUT OIDS");
                    actionGenerated = true;
                    break;
                    
                case SET_LOGGED_UNLOGGED:
                    sb.append("SET ");
                    sb.append(Randomly.fromOptions("LOGGED", "UNLOGGED"));
                    errors.add("because it is temporary");
                    actionGenerated = true;
                    break;
                    
                case NOT_OF:
                    sb.append("NOT OF");
                    errors.add("is not a typed table");
                    actionGenerated = true;
                    break;
                    
                case OWNER_TO:
                    sb.append("OWNER TO ");
                    sb.append(Randomly.fromOptions("CURRENT_USER", "SESSION_USER"));
                    actionGenerated = true;
                    break;
                    
                case REPLICA_IDENTITY:
                    sb.append("REPLICA IDENTITY ");
                    sb.append(Randomly.fromOptions("DEFAULT", "FULL", "NOTHING"));
                    actionGenerated = true;
                    break;
                    
                default:
                    // Skip unknown actions
                    continue;
            }
            
            // Only add statement if an action was actually generated
            if (actionGenerated && sb.length() > lengthBefore) {
                validStatements.add(sb.toString());
            }
        }
        
        // If no valid statements were generated, skip this generation
        if (validStatements.isEmpty()) {
            throw new IgnoreMeException();
        }
        
        return new SQLQueryAdapter(String.join("; ", validStatements), errors, true);
    }

    private static void alterColumn(PostgresTable randomTable, StringBuilder sb) {
        sb.append("ALTER COLUMN ");
        randomColumn = randomTable.getRandomColumn();
        sb.append(randomColumn.getName());
        sb.append(" ");
    }

    private boolean isIdentityAction(Action a) {
        return a == Action.ALTER_COLUMN_SET_IDENTITY 
            || a == Action.ALTER_COLUMN_SET_GENERATED
            || a == Action.ALTER_COLUMN_DROP_IDENTITY 
            || a == Action.ALTER_COLUMN_RESTART;
    }

    private boolean hasValidColumnsForIdentity(PostgresTable table) {
        return table.getColumns().stream()
            .anyMatch(col -> col.getType() == PostgresDataType.INT);
    }

}
