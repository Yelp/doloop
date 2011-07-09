Tutorial
========

Getting set up
--------------

If you don't have a MySQL server, you'll need to `install and run one <http://dev.mysql.com/doc/refman/5.5/en/installing.html>`_. :py:mod:`doloop` 
uses fairly basic SQL, and should work on MySQL versions as early as 5.0, 
if not earlier.

You'll also want to install the package 
`python-MySQL <http://mysql-python.sourceforge.net/>`_.

Next, you'll want to create at least one table:

.. code-block:: sh

    create-doloop-table biz_loop | mysql -D test # or a db of your choice

You'll want one table per kind of update on kind of thing. For example, if you 
want to separately update users' profile pages and their friend 
recommendations, you'd want two tables, named something like 
``user_profile_loop`` and ``user_friend_loop``.

By default, :py:mod:`doloop` assumes IDs are ``INTs``, but you can
actually use any column type that can be a primary key. For example,
if your IDs are 64-character ASCII strings:

.. code-block:: sh

    create-doloop-table -i 'CHAR(64) CHARSET ascii' | mysql -D test

You can also create tables programmatically using :py:func:`doloop.create` and 
:py:func:`doloop.sql_for_create`.


Adding things to the loop
-------------------------

Use :py:func:`doloop.add` to add IDs::

    dbconn = MySQLdb.connection(...)

    for user_id in ...: # your function to stream all user IDs
        doloop.add(dbconn, 'user_loop', user_id)

You'll also want to add a call to :py:func:`doloop.add` to your user creation 
code. :py:func:`doloop.add` uses ``INSERT IGNORE``, so it's fine to call 
it several times for the same ID.

Each call to :py:func:`doloop.add` does a database transaction, so it's 
actually much more efficient to add chunks of several IDs at a time::

    for list_of_user_ids in ...:
        doloop.add(dbconn, 'user_loop', list_of_user_ids)

If something no longer needs to be updated (e.g. the user closes their 
account), you can remove the ID with :py:func:`doloop.remove`.


Doing updates
-------------

The basic workflow is to use :py:func:`doloop.get` to grab the IDs of the 
things that have gone the longest without being updated, perform your updates, 
and then mark them as done with :py:func:`doloop.did`::

    user_ids = doloop.get(dbconn, 'user_loop', 1000)

	for user_id in user_ids:
        ... # run your update logic

	doloop.did(dbconn, 'user_loop', user_ids)

A good, low-effort way to set up workers is to write a script that runs in a
crontab. It's perfectly safe (and encouraged) to run several workers 
concurrently; :py:func:`doloop.get` will lock the IDs it grabs so that other 
workers don't try to update the same things.

You *should* make sure that your update logic can be safely called 
twice concurrently for the same ID. In fact, it's totally cool for code that 
has never called :py:func:`doloop.get` to update arbitrary things and then call 
:py:func:`~doloop.did` on their IDs to let the workers know.

How many workers you want and when they run is up to you. If 
there turn out not to be enough workers, things will simply be updated less 
often than you'd like. You *can* set a limit on how frequently the same ID 
will be updated; by default, this is one hour.

Also, don't worry too much about your workers crashing. By default, IDs are 
locked for an hour, so they'll eventually get unlocked and fetched by 
another worker. Conversely, if there is a problem ID that always causes a 
crash, that problem ID won't bother your workers for another hour. You can 
also explicitly unlock IDs with :py:func:`doloop.unlock`.


Prioritization
--------------

So, this is a great system for making sure every user gets updated eventually, 
but some users are more active than others. You can use :py:func:`doloop.bump` 
to prioritize certain ID(s)::

    def user_do_something_noteworthy(user_id):
        ... # your logic for the user doing something noteworthy

        doloop.bump(dbconn, 'user_loop', user_id)

:py:mod:`doloop` has an elegant (or depending how you look at it, too-magical)
rule that IDs which are locked get highest priority *once the lock expires*. 
By default, :py:func:`~doloop.bump` sets the lock to expire immediately, so 
we get priority without any waiting.

However, in real life, users are likely to do several noteworthy things in 
one session (depending on your users). You can avoid updating the same user 
several times by setting *lock_for*. For example, the first time a user 
does something noteworthy, this code will wait for an hour, and then update 
them::

    def user_do_something_noteworthy(user_id):
        ...

        doloop.bump(dbconn, 'user_loop', user_id, lock_for=60*60)

If a particularly special user did noteworthy things continuously, they'd 
still get updated more or less hourly; you can't repeatedly 
:py:func:`~doloop.bump` things into the future.

If for some reason you forgot to add a user, :py:func:`~doloop.bump` will 
automatically add them before bumping them (as will :py:func:`~doloop.did` 
and :py:func:`~doloop.unlock`). An alternate way to use :py:mod:`doloop` 
is to :py:func:`~doloop.bump` every time something changes, secure in the 
knowledge that if you forgot to add a call to :py:func:`~doloop.bump` 
somewhere, things will still get updated eventually.

Also, due to :py:mod:`doloop`'s elegant/too-magical semantics, you can give 
ID(s) super-high priority by setting *lock_for* to a negative number. At a 
certain point, though, you should just do the update immediately and call 
:py:func:`~doloop.did`.


Auditing
--------

If you want to check on a particular ID or set of IDs, for example to see how 
long it's gone without being updated, you can use :py:func:`doloop.check`.

To check on the status of the task loop as a whole, use 
:py:func:`doloop.stats`. Among other things, this can tell you how many IDs
have gone more than a day/week without being updated.
