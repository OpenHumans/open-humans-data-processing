open-humans-data-extraction
===========================

A Flask app for managing the import and packaging of data sets for Open
Humans users.

This app is currently designed to run on Heroku, using a web and worker dyno
whose configurations are specified in `/Procfile`.

For local development, running this app with `foreman` is strongly recommended,
as well as a `\.env` file containing environment variable values (see
`env.example`).

### Setting up the cache database

Running this site requires a PostgreSQL database (even for local development).

- In Debian/Ubuntu
  - Become the postgres user: `sudo su - postgres`
  - Create a database (example name 'mydb'): `createdb mydb`
  - Create a user (example user 'jdoe'): `createuser -P jdoe`
  - Enter the password at prompt (example password: 'pa55wd')
  - run PostgreSQL command line: `psql`
    - Give this user privileges on this database, e.g.:<br>
      `GRANT ALL PRIVILEGES ON DATABASE mydb TO jdoe;`
    - Also allow this user to create new databases (needed for running tests),
      e.g.:<br>
      `ALTER USER jdoe CREATEDB;`
    - Quit: `\q`
  - Exit postgres user login: `exit`

Specify the Postgres connection string in your `.env` file, e.g.:

```sh
DATABASE_URL="postgres://jdoe:pa55wd@127.0.0.1/mydb"
```

Create the database tables like so:

```sh
$ foreman run python

>>> from utilities import init_db
>>> db = init_db()
>>> db.create_all()
```

### Setting up a Redis server for requests-respectful

The requests-respectful package requires a Redis server.

To set this up in Ubuntu using apt-get:
```
sudo apt-get install redis-server
```

To set this up in OSX using brew:
```
brew install redis
brew services start redis
```

Default configurations should work fine.

### Notes on S3 Bucket Permissions

Putting these here for future reference, for understanding best practices in
using S3 storage.

* Create an IAM user that only has permissions necessary for a given deployment.
E.g. if you're doing local development, make a bucket for yourself and an IAM
user for your local development.
* We use the boto package to work with S3 buckets. `boto.get_bucket` performs a
"HEAD Bucket" operation to check that a bucket exists. This requires the
"s3:ListBucket" permission. It's also an option to run the command with
`validate=False`, but this might produce confusing errors when a bucket doesn't
exist.
* S3 permission policies must be split according to bucket-level permissions
(using an ARN like `arn:aws:s3:::my-bucket-name`) and object-level permissions
(using an ARN like `arn:aws:s3:::my-bucket-name/*`).


### Testing

***(Note: this takes a long time to run, and some tests demand nontrivial
  resources from other sites/repositories/etc. We should consider splitting
  up the tests, only running tests on updated aspects.)***

Tests can be run using nosetests, e.g. `nosetests data_retrieval/tests.py`.
