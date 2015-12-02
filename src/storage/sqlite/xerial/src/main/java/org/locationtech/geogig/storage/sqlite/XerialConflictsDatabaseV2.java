/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Justin Deoliveira (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import static java.lang.String.format;
import static org.locationtech.geogig.storage.sqlite.Xerial.log;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.locationtech.geogig.storage.sqlite.SQLiteTransactionHandler.WriteOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * Conflicts database based on Xerial SQLite jdbc driver.
 * 
 * @author Justin Deoliveira, Boundless
 */
class XerialConflictsDatabaseV2 extends SQLiteConflictsDatabase<DataSource> {

    final static Logger LOG = LoggerFactory.getLogger(XerialConflictsDatabaseV2.class);

    final static String CONFLICTS = "conflicts";

    private SQLiteTransactionHandler txHandler;

    @Inject
    public XerialConflictsDatabaseV2(DataSource ds, SQLiteTransactionHandler txHandler) {
        super(ds);
        this.txHandler = txHandler;
    }

    @Override
    protected void init(DataSource ds) {
        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws SQLException {
                String sql = format("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR, "
                        + "path VARCHAR, conflict VARCHAR, PRIMARY KEY(namespace,path))", CONFLICTS);

                cx.setAutoCommit(false);
                try (Statement statement = cx.createStatement()) {
                    statement.execute(log(sql, LOG));
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    throw e;
                }

                return null;
            }
        };
        txHandler.runTx(op);
    }

    @Override
    protected int count(final String namespace, DataSource ds) {
        Integer count = new DbOp<Integer>() {
            @Override
            protected Integer doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT count(*) FROM %s WHERE namespace = ?", CONFLICTS);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace))) {
                    ps.setString(1, namespace);
                    int count = 0;
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            count = rs.getInt(1);
                        }
                    }
                    return Integer.valueOf(count);
                }
            }
        }.run(ds);

        return count.intValue();
    }

    @Override
    protected Iterable<String> get(final String namespace, final String pathFilter, DataSource ds) {
        Connection cx = Xerial.newConnection(ds);
        ResultSet rs = new DbOp<ResultSet>() {
            @Override
            protected ResultSet doRun(Connection cx) throws IOException, SQLException {
                String sql = format(
                        "SELECT conflict FROM %s WHERE namespace = ? AND path LIKE '%%%s%%'",
                        CONFLICTS, pathFilter);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace))) {
                    ps.setString(1, namespace);

                    return ps.executeQuery();
                }
            }
        }.run(cx);

        return new StringResultSetIterable(rs, cx);
    }

    @Override
    protected void put(final String namespace, final String path, final String conflict,
            DataSource ds) {

        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT OR REPLACE INTO %s VALUES (?,?,?)", CONFLICTS);

                log(sql, LOG, namespace, path, conflict);

                cx.setAutoCommit(false);
                try (PreparedStatement ps = cx.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, path);
                    ps.setString(3, conflict);

                    ps.executeUpdate();
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    throw e;
                }
                return null;
            }
        };
        txHandler.runTx(op);
    }

    @Override
    protected void remove(final String namespace, final String path, DataSource ds) {
        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("DELETE FROM %s WHERE namespace = ? AND path = ?", CONFLICTS);

                log(sql, LOG, namespace, path);

                cx.setAutoCommit(false);
                try (PreparedStatement ps = cx.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, path);
                    ps.executeUpdate();
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    throw e;
                }
                return null;
            }
        };
        txHandler.runTx(op);
    }

}
