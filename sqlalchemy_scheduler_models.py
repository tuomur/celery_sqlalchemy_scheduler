# -*- coding: utf-8 -*-

import datetime
import json

import sqlalchemy
from sqlalchemy import Column, String, Integer, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.event
from celery import schedules

engine = sqlalchemy.create_engine('sqlite://')
Base = declarative_base(bind=engine)


class CrontabSchedule(Base):
    __tablename__ = 'celery_crontabs'

    id = Column(Integer, primary_key=True)
    minute = Column(String(64), default='*')
    hour = Column(String(64), default='*')
    day_of_week = Column(String(64), default='*')
    day_of_month = Column(String(64), default='*')
    month_of_year = Column(String(64), default='*')

    @property
    def schedule(self):
        return schedules.crontab(minute=self.minute,
                                 hour=self.hour,
                                 day_of_week=self.day_of_week,
                                 day_of_month=self.day_of_month,
                                 month_of_year=self.month_of_year)

    @classmethod
    def from_schedule(cls, dbsession, schedule):
        spec = {'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year}
        try:
            query = dbsession.query(CrontabSchedule)
            query = query.filter_by(**spec)
            existing = query.one()
            return existing
        except NoResultFound:
            return cls(**spec)
        except MultipleResultsFound:
            query = dbsession.query(CrontabSchedule)
            query = query.filter_by(**spec)
            query.delete()
            dbsession.commit()
            return cls(**spec)


class IntervalSchedule(Base):
    __tablename__ = 'celery_intervals'

    id = Column(Integer, primary_key=True)
    every = Column(Integer, nullable=False)
    period = Column(String(24))

    @property
    def schedule(self):
        return schedules.schedule(datetime.timedelta(**{self.period: self.every}))

    @classmethod
    def from_schedule(cls, dbsession, schedule, period='seconds'):
        every = max(schedule.run_every.total_seconds(), 0)
        try:
            query = dbsession.query(IntervalSchedule)
            query = query.filter_by(every=every, period=period)
            existing = query.one()
            return existing
        except NoResultFound:
            return cls(every=every, period=period)
        except MultipleResultsFound:
            query = dbsession.query(IntervalSchedule)
            query = query.filter_by(every=every, period=period)
            query.delete()
            dbsession.commit()
            return cls(every=every, period=period)


class DatabaseSchedulerEntry(Base):
    __tablename__ = 'celery_schedules'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    task = Column(String(255))
    interval_id = Column(Integer, ForeignKey('celery_intervals.id'))
    crontab_id = Column(Integer, ForeignKey('celery_crontabs.id'))
    arguments = Column(String(255), default='[]')
    keyword_arguments = Column(String(255), default='{}')
    queue = Column(String(255))
    exchange = Column(String(255))
    routing_key = Column(String(255))
    expires = Column(DateTime)
    enabled = Column(Boolean, default=True)
    last_run_at = Column(DateTime)
    total_run_count = Column(Integer, default=0)
    date_changed = Column(DateTime)

    interval = relationship(IntervalSchedule)
    crontab = relationship(CrontabSchedule)

    @property
    def args(self):
        return json.loads(self.arguments)

    @args.setter
    def args(self, value):
        self.arguments = json.dumps(value)

    @property
    def kwargs(self):
        return json.loads(self.keyword_arguments)

    @kwargs.setter
    def kwargs(self, kwargs_):
        self.keyword_arguments = json.dumps(kwargs_)

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        if self.crontab:
            return self.crontab.schedule


@sqlalchemy.event.listens_for(DatabaseSchedulerEntry, 'before_insert')
def _set_entry_changed_date(mapper, connection, target):
    target.date_changed = datetime.datetime.utcnow()
