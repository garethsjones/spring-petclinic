package org.springframework.samples.petclinic.owner.dao;

import static java.util.Spliterators.*;
import static java.util.stream.StreamSupport.*;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Collector;

public class ResultSetCollector<A, B, C> implements ResultSetExtractor<C> {

    private final RowMapper<A> rowMapper;
    private final Function<A, B> operation;
    private final Collector<B, ?, C> collector;

    public ResultSetCollector(RowMapper<A> rowMapper, Function<A, B> operation, Collector<B, ?, C> collector) {
        this.rowMapper = rowMapper;
        this.operation = operation;
        this.collector = collector;
    }

    @Override
    public C extractData(ResultSet resultSet) throws DataAccessException {

        ResultSetIterator<A> resultSetIterator = new ResultSetIterator<>(resultSet, rowMapper);

        return stream(spliteratorUnknownSize(resultSetIterator, Spliterator.ORDERED), false)
            .onClose(() -> {
                try {
                    resultSetIterator.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).map(operation).collect(collector);
    }
}
