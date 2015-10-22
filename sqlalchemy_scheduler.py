# -*- coding: utf-8 -*-

"""
Most of this code is lifted from django-celery project and adapted to run on
SQLAlchemy.
"""

from __future__ import absolute_import
import datetime
import time

from sqlalchemy.orm import sessionmaker

from celery.beat import Scheduler, ScheduleEntry
from celery import schedules, current_app
from celery.utils.timeutils import is_naive

from sqlalchemy_scheduler_models import DatabaseSchedulerEntry, CrontabSchedule, IntervalSchedule


# The schedule objects need to be handled within one scope
Session = sessionmaker(autocommit=False, autoflush=False)
dbsession = Session()


class Entry(ScheduleEntry):
    model_schedules = ((schedules.crontab, CrontabSchedule, 'crontab'),
                       (schedules.schedule, IntervalSchedule, 'interval'))

    def __init__(self, model):
        self.app = current_app._get_current_object()
        self.name = model.name
        self.task = model.task
        self.schedule = model.schedule
        self.args = model.args
        self.kwargs = model.kwargs
        self.options = dict(
            queue=model.queue,
            exchange=model.exchange,
            routing_key=model.routing_key,
            expires=model.expires,
        )
        self.total_run_count = model.total_run_count
        self.model = model

        if not model.last_run_at:
            model.last_run_at = self._default_now()
        orig = self.last_run_at = model.last_run_at
        if not is_naive(self.last_run_at):
            self.last_run_at = self.last_run_at.replace(tzinfo=None)
        assert orig.hour == self.last_run_at.hour  # timezone sanity

    def is_due(self):
        if not self.model.enabled:
            return False, 5.0   # 5 second delay for re-enable.
        return self.schedule.is_due(self.last_run_at)

    def _default_now(self):
        return datetime.datetime.utcnow()

    def __next__(self):
        self.model.last_run_at = self._default_now()
        self.model.total_run_count += 1
        dbsession.commit()
        return self.__class__(self.model)
    next = __next__  # for 2to3

    @classmethod
    def to_model_schedule(cls, schedule):
        for schedule_type, model_type, model_field in cls.model_schedules:
            schedule = schedules.maybe_schedule(schedule)
            if isinstance(schedule, schedule_type):
                model_schedule = model_type.from_schedule(dbsession, schedule)
                return model_schedule, model_field
        raise ValueError(
            'Cannot convert schedule type {0!r} to model'.format(schedule))

    @classmethod
    def from_entry(cls, name, skip_fields=('relative', 'options'), **entry):
        options = entry.get('options') or {}
        fields = dict(entry)
        for skip_field in skip_fields:
            fields.pop(skip_field, None)
        schedule = fields.pop('schedule')
        model_schedule, model_field = cls.to_model_schedule(schedule)
        fields[model_field] = model_schedule
        fields['args'] = fields.get('args') or []
        fields['kwargs'] = fields.get('kwargs') or {}
        fields['queue'] = options.get('queue')
        fields['exchange'] = options.get('exchange')
        fields['routing_key'] = options.get('routing_key')

        query = dbsession.query(DatabaseSchedulerEntry)
        query = query.filter_by(name=name)
        db_entry = query.first()
        if db_entry is None:
            new_entry = DatabaseSchedulerEntry(**fields)
            new_entry.name = name
            dbsession.add(new_entry)
            dbsession.commit()
            db_entry = new_entry
        return cls(db_entry)


class DatabaseScheduler(Scheduler):
    Entry = Entry
    _last_timestamp = None
    _schedule = None
    _initial_read = False

    def __init__(self, app, **kwargs):
        self._last_timestamp = self._get_latest_change()
        Scheduler.__init__(self, app, **kwargs)

    def _get_latest_change(self):
        query = dbsession.query(DatabaseSchedulerEntry.date_changed)
        query = query.order_by(DatabaseSchedulerEntry.date_changed.desc())
        latest_entry_date = query.first()
        return latest_entry_date

    def setup_schedule(self):
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.CELERYBEAT_SCHEDULE)

    def _all_as_schedule(self):
        s = {}
        query = dbsession.query(DatabaseSchedulerEntry)
        query = query.filter_by(enabled=True)
        for row in query:
            s[row.name] = Entry(row)
        return s

    def schedule_changed(self):
        ts = self._get_latest_change()
        if ts > self._last_timestamp:
            self._last_timestamp = ts
            return True

    def update_from_dict(self, dict_):
        s = {}
        for name, entry in dict_.items():
            try:
                s[name] = self.Entry.from_entry(name, **entry)
            except Exception as exc:
                self.logger.exception('update_from_dict')
        self.schedule.update(s)

    def tick(self):
        self.logger.debug('DatabaseScheduler: tick')
        Scheduler.tick(self)
        if self.should_sync():
            self.sync()
        return 5  # sleep time until next tick

    def should_sync(self):
        sync_reason_time = (time.time() - self._last_sync) > self.sync_every
        sync_reason_task_count = self.sync_every_tasks and self._tasks_since_sync >= self.sync_every_tasks
        bool_ = sync_reason_time or sync_reason_task_count
        self.logger.debug('DatabaseScheduler: should_sync: {0}'.format(bool_))
        return bool_

    def sync(self):
        self._last_sync = time.time()
        self.logger.debug('DatabaseScheduler: sync')
        self._schedule = self._all_as_schedule()

    @property
    def schedule(self):
        update = False
        if not self._initial_read:
            self.logger.debug('DatabaseScheduler: intial read')
            update = True
            self._initial_read = True
        elif self.schedule_changed():
            self.logger.info('DatabaseScheduler: Schedule changed.')
            update = True

        if update:
            self.sync()
        return self._schedule
