package org.springframework.samples.petclinic.owner.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

@Component
public class PetsDao {

    private static final Logger log = LoggerFactory.getLogger(PetsDao.class);

    private final JdbcTemplate jdbcTemplate;

    private static final String SQL = "" +
        "SELECT \n" +
        "    o.first_name,\n" +
        "    o.last_name,\n" +
        "    o.address,\n" +
        "    o.city,\n" +
        "    o.telephone,\n" +
        "    p.name AS pet,\n" +
        "    t.name AS type,\n" +
        "    p.birth_date,\n" +
        "    now() AS export_date\n" +
        "FROM\n" +
        "    petclinic.owners o\n" +
        "JOIN petclinic.pets p ON (p.owner_id = o.id)\n" +
        "JOIN petclinic.types t ON (p.type_id = t.id)\n" +
        "ORDER BY o.last_name, o.first_name, o.id";

    private static final RowMapper<List<String>> ROW_MAPPER = (rs, rowNum) -> {

        List<String> row = new ArrayList<>();

        row.add(rs.getString("first_name"));
        row.add(rs.getString("last_name"));
        row.add(rs.getString("address"));
        row.add(rs.getString("city"));
        row.add(rs.getString("telephone"));
        row.add(rs.getString("pet"));
        row.add(rs.getString("type"));
        row.add(rs.getString("birth_date"));
        row.add(rs.getString("export_date"));

        return row;
    };

    @Autowired
    public PetsDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<List<String>> fetch() {

        List<List<String>> rows = jdbcTemplate.query(SQL, new Object[]{}, ROW_MAPPER);

        return rows;
    }

    public List<List<String>> fetch(int limit, int offset) {

        String sql = String.format("%s LIMIT %s OFFSET %s", SQL, limit, offset);

        List<List<String>> rows = jdbcTemplate.query(sql, new Object[]{}, ROW_MAPPER);

        return rows;
    }

    public <F, D> D stream(Function<List<String>, F> operation, Collector<F, ?, D> collector) {

        ResultSetCollector<List<String>, F, D> resultSetCollector = new ResultSetCollector<>(ROW_MAPPER, operation, collector);

        return jdbcTemplate.query(SQL, new Object[]{}, resultSetCollector);
    }

    public Stream<List<String>> fetchStream() {

        return jdbcTemplate.query(SQL, new Object[]{}, new ResultSetStreamer(ROW_MAPPER));
    }
}
