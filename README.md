# What

This repo contains my modification of the Celery Database Scheduler from
djcelery project. I've used it in production over a year now without problems.


## How do I use this

You'll have to include the scheduler module in your project and modify the
models to work with your SQLAlchemy setup. The code in repo just uses temporary
in-memory SQLite database since I cannot assume anything.

Finally, set `CELERYBEAT_SCHEDULER` to
`yourproject.sqlalchemy_scheduler:DatabaseScheduler`.
