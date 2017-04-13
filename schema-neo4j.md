Constraints for the neo4j database

Indexes:
    ON :filename(name) ONLINE (for uniqueness constraint)
    ON :word(name) ONLINE (for uniqueness constraint)

Constraints:
   ON ( filename:filename ) ASSERT filename.name IS UNIQUE
   ON ( word:word ) ASSERT word.name IS UNIQUE
