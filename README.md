# mysql_table_optimizer
A simple tool in Python for optimizing a list of MySQL tables. It takes care of replication lag as well as when it runs, it takes the small tables first so that more disk blocks can be freed for bigger tables.

Usage: python mysql_table_optimizer.pl -f <filename containing the list of tables in DBname.tablename format>
