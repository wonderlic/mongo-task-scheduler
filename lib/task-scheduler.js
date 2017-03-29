var _ = require('lodash');
var Q = require('q');
var moment = require('moment-timezone');
var cronParser = require('cron-parser');

// TODO... what indexes should be created and should this be responsible for creating them?

function TaskScheduler() {
  var self = this;

  self.errorHandler = console.error;

  self.databasePromise = null;
  self.collectionName = '_scheduledTasks';

  self.timezone = moment.tz.guess();

  self.pollingInterval = 1000; // 1 second by default
  self.processingTimeout = 60 * 1000; // 1 minute by default

  self._taskWorkers = {};
  self._pollingIntervalId = null;

  self.scheduleTask = function(id, cronSchedule, worker) {
    self._taskWorkers[id] = worker;
    self._startPolling();

    return self._findTask(id)
      .then(function(task) {
        var interval = cronParser.parseExpression(cronSchedule, {tz: self.timezone});
        var nextTime = interval.next().toDate();
        if (!task) {
          console.log('next scheduled time for task \'' + id + '\' is ' + _getFormattedTime(nextTime));
          return self._insertTask({
            _id: id,
            cronSchedule: cronSchedule,
            nextScheduledTime: nextTime
          });
        } else {
          if (task.cronSchedule !== cronSchedule || (!task.receivedTime && task.nextScheduledTime > nextTime)) {
            console.log('next scheduled time for task \'' + task._id + '\' is ' + _getFormattedTime(nextTime));
            return self._updateTask(task, {$set: {cronSchedule: cronSchedule, nextScheduledTime: nextTime}});
          } else {
            console.log('next scheduled time for task \'' + task._id + '\' is ' + _getFormattedTime(task.nextScheduledTime));
          }
        }
      });
  };

  self.stopPolling = function() {
    self._stopPolling();
  };

  //region Private Helper Methods

  self._startPolling = function() {
    if (!self._pollingIntervalId) {
      self._pollingIntervalId = setInterval(self._poll, self.pollingInterval);
    }
  };

  self._stopPolling = function() {
    if (self._pollingIntervalId) {
      clearInterval(self._pollingIntervalId);
    }
  };

  self._poll = function() {
    return self._receiveTask()
      .then(function(task) {
        if (!task) { return; }

        return Q(self._processTask(task))
          .then(function(output) {
            return self._completeTask(task, output);
          })
          .catch(function(err) {
            self.errorHandler(err);
            return self._releaseTask(task, err);
          });
      })
      .catch(self.errorHandler);
  };

  self._processTask = function(task) {
    var worker = self._taskWorkers[task._id];
    if (!worker) { return Q.reject(new Error("No worker registered for scheduled task with id: " + task._id)); }

    return worker();
  };

  self._releaseTask = function(task, err) {
    var now = new Date();
    var interval = cronParser.parseExpression(task.cronSchedule, {
      currentDate: moment().tz(self.timezone).add(1, 'seconds').toDate(),
      tz: self.timezone
    });
    var nextTime = interval.next().toDate();
    console.log('next scheduled time for task \'' + task._id + '\' is ' + _getFormattedTime(nextTime));
    return self._updateTask(task, {
      $unset: {
        receivedTime: true
      },
      $set: {
        nextScheduledTime: nextTime,
        lastErroredTime: now
      },
      $push: {
        executionLog: {
          receivedTime: task.receivedTime,
          erroredTime: now,
          processingTime: now.valueOf() - task.receivedTime.valueOf(),
          errorMessage: err.message
        }
      }
    });
  };

  self._completeTask = function(task, output) {
    var now = new Date();
    var interval = cronParser.parseExpression(task.cronSchedule, {
      currentDate: moment().tz(self.timezone).add(1, 'seconds').toDate(),
      tz: self.timezone
    });
    var nextTime = interval.next().toDate();
    console.log('next scheduled time for task \'' + task._id + '\' is ' + _getFormattedTime(nextTime));
    return self._updateTask(task, {
      $unset: {
        receivedTime: true
      },
      $set: {
        nextScheduledTime: nextTime,
        lastCompletedTime: now
      },
      $push: {
        executionLog: {
          receivedTime: task.receivedTime,
          completedTime: now,
          processingTime: now.valueOf() - task.receivedTime.valueOf(),
          output: _.isEmpty(output) ? undefined : output
        }
      }
    });
  };

  self._receiveTask = function() {
    var query = {
      _id: {$in: _.keys(self._taskWorkers)},
      nextScheduledTime: {$lt: new Date()},
      $or: [
        {receivedTime: {$lt: new Date(Date.now() - self.processingTimeout)}},
        {receivedTime: {$exists: false}}
      ]
    };
    var update = {
      $set: {
        receivedTime: new Date()
      }
    };

    return self._getMongoCollection()
      .then(function(collection) {
        return collection.findOneAndUpdate(query, update, {returnOriginal: false});
      })
      .then(function(result) {
        return result.value;
      });
  };

  self._findTask = function(id) {
    return self._getMongoCollection()
      .then(function(collection) {
        return collection.findOne({_id: id});
      });
  };

  self._updateTask = function(task, update) {
    return self._getMongoCollection()
      .then(function(collection) {
        return [collection, collection.updateOne({_id: task._id}, update)];
      })
      .spread(function(collection) {
        if ((task.executionLog || []).length >= 10) {
          return collection.updateOne({_id: task._id}, {$pop: {executionLog: -1}});
        }
      });
  };

  self._insertTask = function(doc) {
    return self._getMongoCollection()
      .then(function(collection) {
        return collection.insertOne(doc);
      });
  };

  self._getMongoCollection = function() {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName);
      });
  };

  function _getFormattedTime(time) {
    return moment(time).tz(self.timezone).format('YYYY.MM.DD hh:mm:ss A')
  }

  //endregion
}

module.exports = TaskScheduler;
