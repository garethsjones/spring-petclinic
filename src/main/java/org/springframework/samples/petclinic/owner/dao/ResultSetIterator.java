package org.springframework.samples.petclinic.owner.dao;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetIterator<T> implements Iterator<T>, AutoCloseable {

    private final ResultSet resultSet;
    private final RowMapper<T> rowMapper;
    private int rowNum = 0;

    public ResultSetIterator(final ResultSet resultSet, final RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return !resultSet.isClosed() && resultSet.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        try {
            return rowMapper.mapRow(resultSet, rowNum++);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }
}
