BEGIN TRANSACTION;
DROP TABLE IF EXISTS `atest`;
CREATE TABLE atest (
  id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  name TEXT    NOT NULL
);
COMMIT;