# pgaccounts
Create student accounts in a PostgreSQL database from a CSV roster

It's a pain to create student accounts at the beginning of a term, to drop them at the end of the term to start again with a new batch of students.
This small program reads identifiers from a CSV roster, create accounts for every student and creates a schema with the same name as the account owned by the account so that students can create their own tables.
Optionally, one role (the same for everybody) can be granted to every account. This allows to have sample tables in the public schema of the database, and have a role that grants the SELECT privilege on every table in the public schema.

Usage:
```
pgaccounts [operation flag] [flags] <CSV roster>
  Operation flags:
    -C        : Create accounts
    -D        : Delete accounts
  Flags:
    -?        : Display this
    -h <host> : Postgres server (optionally followed by ':port')
    -d <db>   : Create schemas in database <db> (default postgres)
    -U <name> : Postgres superuser
    -i <n>    : Identifier is field <n> (default 1)
    -p <n>    : Initial password is field <n> (default 1)
    -r <role> : Grant <role> to account
    -s <c>    : Set file separator to <c> (default ',')
    -x <n>    : Omit the first <n> lines in the roster (headers)
```
