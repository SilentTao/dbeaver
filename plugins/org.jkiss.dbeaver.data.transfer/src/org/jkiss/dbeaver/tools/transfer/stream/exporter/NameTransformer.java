package org.jkiss.dbeaver.tools.transfer.stream.exporter;

import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPIdentifierCase;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.sql.parser.SQLIdentifierDetector;

import java.util.Arrays;
import java.util.stream.Stream;

class NameTransformer  {
    private static final NameTransformer DEFAULT_INSTANCE = new NameTransformer ();

    protected NameTransformer() {
    }

    public String transformTableName(String name) {
        return this.transformImpl(name);
    }
    public String transformColumnName(String name) {
        return this.transformImpl(name);
    }

    protected String transformImpl(String name) {
        return name;
    }

    public static NameTransformer asIs() {
        return DEFAULT_INSTANCE;
    }
    public static NameTransformer forCase(DBPIdentifierCase identifierCase) {
        return new IdentifierCaseNameTransformer(identifierCase);
    }
    public static NameTransformer forNonQuotedCase(DBPDataSource dataSource, DBPIdentifierCase identifierCase) {
        return new NonQuotedIdentifierCaseNameTransformer(dataSource, identifierCase);
    }

    private static class IdentifierCaseNameTransformer extends NameTransformer {
        protected final DBPIdentifierCase identifierCase;

        public IdentifierCaseNameTransformer(DBPIdentifierCase identifierCase) {
            this.identifierCase = identifierCase;
        }

        @Override
        protected String transformImpl(String name) {
            return this.identifierCase.transform(name);
        }
    }

    private static class NonQuotedIdentifierCaseNameTransformer extends IdentifierCaseNameTransformer {
        private final DBPDataSource dataSource;
        private final SQLIdentifierDetector identifierDetector;

        public NonQuotedIdentifierCaseNameTransformer(DBPDataSource dataSource, DBPIdentifierCase identifierCase) {
            super(identifierCase);
            this.dataSource = dataSource;
            this.identifierDetector = new SQLIdentifierDetector(SQLUtils.getDialectFromDataSource(dataSource));
        }

        @Override
        public String transformTableName(String name) {
            Stream<String> mayBeQualifiedNameParts = Arrays.stream(identifierDetector.splitIdentifier(name));
            return DBUtils.getFullyQualifiedName(dataSource, mayBeQualifiedNameParts.map(this::transformImpl).toArray(String[]::new));
        }

        @Override
        protected String transformImpl(String name) {
            return identifierDetector.isQuoted(name) ? name : this.identifierCase.transform(name);
        }
    }
}
