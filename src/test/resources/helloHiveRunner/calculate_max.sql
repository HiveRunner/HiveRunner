insert into my_schema.result
  select year, max(value) from source_db.test_table group by year;