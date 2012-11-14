# Cassandra [![Build Status](https://secure.travis-ci.org/kreynolds/cassandra-cql.png)](http://travis-ci.org/kreynolds/cassandra-cql) [![Code Climate](https://codeclimate.com/badge.png)](https://codeclimate.com/github/kreynolds/cassandra-cql)
The Apache Cassandra Project (http://cassandra.apache.org) develops a highly scalable second-generation distributed database, bringing together Dynamo's fully distributed design and Bigtable's ColumnFamily-based data model.

# CQL
Cassandra originally went with a Thrift RPC-based API as a way to provide a common denominator that more idiomatic clients could build upon independently.
However, this worked poorly in practice: raw Thrift is too low-level to use productively, and keeping pace with new API methods to support (for example) indexes in 0.7 or distributed counters in 0.8 is too much for many maintainers.

CQL, the Cassandra Query Language, addresses this by pushing all implementation details to the server; all the client has to know for any operation is how to interpret "resultset" objects.
So adding a feature like counters just requires teaching the CQL parser to understand "column + N" notation; no client-side changes are necessary.

(CQL Specification: http://cassandra.apache.org/doc/cql/CQL.html)

# Quick Start

## Establishing a connection

    # Defaults to the system keyspace
    db = CassandraCQL::Database.new('127.0.0.1:9160')

    # Specifying a keyspace
    db = CassandraCQL::Database.new('127.0.0.1:9160', {:keyspace => 'keyspace1'})

    # Specifying more than one seed node
    db = CassandraCQL::Database.new(['127.0.0.1:9160','127.0.0.2:9160'])
  
## Creating a Keyspace

    # Creating a simple keyspace with replication factor 1
    db.execute("CREATE KEYSPACE keyspace1 WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor=1")
    db.execute("USE keyspace1")

## Creating a Column Family

    # Creating a column family with a single validated column
    db.execute("CREATE COLUMNFAMILY users (id varchar PRIMARY KEY, email varchar)")

    # Create an index on the name
    db.execute("CREATE INDEX users_email_idx ON users (email)")

## Inserting into a Column Family

    # Insert without bound variables
    db.execute("INSERT INTO users (id, email) VALUES ('kreynolds', 'kelley@insidesystems.net')")

    # Insert with bound variables
    db.execute("INSERT INTO users (id, email) VALUES (?, ?)", 'kway', 'kevin@insidesystems.net')
  
## Updating a Column Family

    # Update
    db.execute("UPDATE users SET email=? WHERE id=?", 'kreynolds@insidesystems.net', 'kreynolds')
  
## Selecting from a Column Family

    # Select all
    db.execute("SELECT * FROM users").fetch { |row| puts row.to_hash.inspect }
      {"id"=>"kway", "email"=>"kevin@insidesystems.net"}
      {"id"=>"kreynolds", "email"=>"kreynolds@insidesystems.net"}

    # Select just one user by id
    db.execute("SELECT * FROM users WHERE id=?", 'kreynolds').fetch { |row| puts row.to_hash.inspect }
      {"id"=>"kreynolds", "email"=>"kreynolds@insidesystems.net"}

    # Select just one user by indexed column
    db.execute("SELECT * FROM users WHERE email=?", 'kreynolds@insidesystems.net').fetch { |row| puts row.to_hash.inspect }
      {"id"=>"kreynolds", "email"=>"kreynolds@insidesystems.net"}
  
## Deleting from a Column Family

    # Delete the swarthy bastard Kevin
    db.execute("DELETE FROM users WHERE id=?", 'kway')

# Notes

## Changing Validation on Columns with existing/unvalidatable data

  If you have existing data and change the validation on a column in an incompatible
  way (ie. blank strings with a column validated as Integer), a CastException will be raised.
  The exception has a 'bytes' attribute that will give you access to the bytes that caused the problem.
  
  Other columns in a row can still be accessible via index or column_name without raising that exception.