Cyan Audit 0.4
==============

Cyan Audit is a powerful extension for in-database logging of DDL.


Introduction
------------

How do you keep track of who modified the contents of your database?

Most of the time, such logging is implemented in the application layer, meaning
that every action your application takes has to have extra code just to log the
action. Therefore, if you forget to add the code to log the action, it will
never be logged! This is a big headache for the application developer who just
wants to get the code written.

Cyan Audit aims to solve this problem by providing an easy and powerful logging
system that requires no modification to your application and is installed easily
and cleanly as a PostgreSQL extension.

Cyan Audit can selectively log DDL on a column-by-column basis, and you can
select which tables/columns to log using a simple UPDATE command.

You can also turn off logging entirely for just your session if you'd like to
perform bulk administrative actions without clogging up your log table.

The contents of the log are available through a view which can be queried easily
based on recorded timestamp, table/column, userid who performed the action, PK
value of the affected row, and more.

One of the handiest features, however, is the ability to "undo" a transaction. A
simple function call will issue SQL statements to reverse every data
modification logged for the given transaction ID.

Does that sound interesting? Good, let's get started.


Installation
------------

Unpack the source files into a directory in your file system:

    tar zxvf cyanaudit-0.4.tar.gz

Now go into the directory and simply run `make install`. You will have to have
pg_config in your path in orderr for this to work. You can see if it is in your
path by issuing the command 


vim: ft=txt


