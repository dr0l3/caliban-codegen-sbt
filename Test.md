# What do we need

We would like to execute the following sequence of tasks

1. pgCalibanGenerate
2. compile
3. test

We would like to run these against an externally running postgres instance with configurable sql

## Setting up the database

If we have defined a file `init.sql` then we can run the external docker container using this

```shell
docker run \ 
  -v "$pwd/init.sql:/sql/init.sql" \
  -e POSTGRES_PASSWORD=postgres \
  -p 15555:5432 \
  postgres:14
```

