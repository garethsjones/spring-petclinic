package org.springframework.samples.petclinic.owner.dao;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class ResultSetStreamer implements ResultSetExtractor<Stream<List<String>>> {

    private final RowMapper<List<String>> rowMapper;

    public ResultSetStreamer(RowMapper<List<String>> rowMapper) {
        this.rowMapper = rowMapper;
    }

    @Override
    public Stream<List<String>> extractData(ResultSet resultSet) throws SQLException, DataAccessException {

        ResultSetIterator<List<String>> resultSetIterator = new ResultSetIterator<>(resultSet, rowMapper);

        return stream(spliteratorUnknownSize(resultSetIterator, Spliterator.ORDERED), false);
    }
}
