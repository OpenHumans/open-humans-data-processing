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

Specify a Postgres connection string in your `.env` file, e.g.:

```sh
DATABASE_URL="postgres://ohadmin:ohpassword@127.0.0.1/ohprocessing"
```

Create the database tables like so:

```sh
$ foreman run python

>>> from utilities import init_db
>>> db = init_db()
>>> db.create_all()
```

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
