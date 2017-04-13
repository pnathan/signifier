Signifier
===

indexes files whose names are given on stdin and places them into a
Neo4J database. Generally assumes these files are *text*, largely for
reasons of simplicity and longjevity.

Putative headline might be: the signifier is a highly multithreaded
theoretically performant tool to index your textfiles for future use.

Typical usage.


    find "~/textfiles" | /path/to/signifier



A query might be:

MATCH p=(f)-[r:CONTAINS]->(w {name: 'code'}) return f.name;

Tools for looking up information have not been created.

The database in Neo4J will need constraints.

TODO:
---

* The filename nodes need to have path, hash, and name

* The tool should take command line options to nice itself down for
use in cron

* Plausibly emitting a CSV and then using a Neo4J bulk-loading tool
would be significantly faster.

* Use the Bolt API somehow, not the REST API.




copyright 2015-2017 *AGPL3*, Paul Nathan
