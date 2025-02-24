import moment from 'moment-timezone';
import cronParser from 'cron-parser';

import type {Db as MongoDb, Collection as MongoCollection, Filter as MongoFilter, UpdateFilter as MongoUpdateFilter} from 'mongodb';


type TaskWorker = (taskInstance: TaskInstance) => Promise<string | undefined> | string | undefined;

interface TaskInstance {
  /// This method should be called by the task worker to signal that the task is making progress.
  /// Useful for long-running tasks that would otherwise be considered timed out.
  signalProgress(): void;
}

type TaskExecutionLogEntry = {receivedTime: Date; processingTime: number} & (
  {erroredTime: Date; errorMessage: string} | 
    {completedTime: Date; output?: string});

interface TaskDefinition {
  _id: string;
  cronSchedule: string;
  nextScheduledTime: Date;
  receivedTime?: Date;
  lastErroredTime?: Date;
  lastCompletedTime?: Date;
  executionLog?: Array<TaskExecutionLogEntry>;
}

export class TaskScheduler {
  private taskWorkers: Map<string, TaskWorker> = new Map();
  private pollingIntervalId: number | undefined = undefined;

  // Options
  public pollingInterval: number = 1000; // 1 second
  public processingTimeout: number = 60 * 1000; // 1 minute
  public timezone: string = moment.tz.guess();
  public errorHandler = console.error;
  public collectionName = '_scheduledTasks';
  public databasePromise: (() => Promise<MongoDb>) | undefined;

  async scheduleTask(id: string, cronSchedule: string, taskWorker: TaskWorker) {
    this.taskWorkers.set(id, taskWorker);
    this.startPolling();

    const task = await this.findTask(id);

    const interval = cronParser.parseExpression(cronSchedule, {tz: this.timezone});
    const nextTime = interval.next().toDate();

    if (!task) {
      console.log(`next scheduled time for task '${id}' is ${this.getFormattedTime(nextTime)}`);
      return this.insertTask({
        _id: id,
        cronSchedule,
        nextScheduledTime: nextTime
      });
    } else {
      if (task.cronSchedule !== cronSchedule) {
        console.log(`next scheduled time for task '${task._id}' is ${this.getFormattedTime(nextTime)}`);
        return this.updateTask(task, {
          $set: {
            cronSchedule,
            nextScheduledTime: nextTime
          }
        });
      } else {
        console.log(`next scheduled time for task '${task._id}' is ${this.getFormattedTime(task.nextScheduledTime)}`);

      }
    }
  }

  public stopPolling() {
    if (this.pollingIntervalId) {
      clearInterval(this.pollingIntervalId);
      this.pollingIntervalId = undefined;
    }
  }

  // Private Methods
  private async findTask(id: string): Promise<TaskDefinition | undefined> {
    const collection = await this.getMongoCollection()

    const result = await collection.findOne({
      _id: id
    });

    return result ?? undefined;
  }

  private startPolling() {
    if (!this.pollingIntervalId) {
      this.pollingIntervalId = setInterval(this.poll, this.pollingInterval);
    }
  }

  private poll = async () => {
    const task = await this.receiveTask();

    if (!task) {
      return;
    }

    try {
      const output = await this.processTask(task);
      return this.completeTask(task, output);
    } catch (error: any) {
      this.errorHandler(error);
      return this.releaseTask(task, error);
    }
  }

  private async processTask(task: TaskDefinition): Promise<string | undefined> {
    const taskWorker = this.taskWorkers.get(task._id);
    if (!taskWorker) {
      throw new Error(`No worker registered for scheduled task with id: ${task._id}`);
    }

    return taskWorker({
      signalProgress: () => {
        return this.updateTask(task, {
          $set: {
            receivedTime: new Date()
          }
        });
      }
    });
  }

  private releaseTask(task: TaskDefinition, error: Error) {
    const now = new Date();
    const interval = cronParser.parseExpression(task.cronSchedule, {
      currentDate: moment().tz(this.timezone).add(1, 'seconds').toDate(),
      tz: this.timezone});

    const nextTime = interval.next().toDate();
    console.log(`next scheduled time for task '${task._id}' is ${this.getFormattedTime(nextTime)}`);

    const receivedTime = task.receivedTime ?? new Date();

    return this.updateTask(task, {
      $unset: {
        receivedTime: true,
      },
      $set: {
        nextScheduledTime: nextTime,
        lastErroredTime: now,
      },
      $push: {
        executionLog: {
          receivedTime,
          erroredTime: now,
          processingTime: now.valueOf() - receivedTime.valueOf(),
          errorMessage: error.message,
        }
      }
    });
  }

    private completeTask(task: TaskDefinition, output: string | undefined) {
      const now = new Date();
      const interval = cronParser.parseExpression(task.cronSchedule, {
        currentDate: moment().tz(this.timezone).add(1, 'seconds').toDate(),
        tz: this.timezone,
      });
    const nextTime = interval.next().toDate();
    console.log(`next scheduled time for task '${task._id}' is ${this.getFormattedTime(nextTime)}`);

    const receivedTime = task.receivedTime ?? new Date();

    return this.updateTask(task, {
      $unset: {
        receivedTime: true,
      },
      $set: {
        nextScheduledTime: nextTime,
        lastCompletedTime: now,
      },
      $push: {
        executionLog: {
          receivedTime,
          completedTime: now,
          processingTime: now.valueOf() - receivedTime.valueOf(),
          output,
        }
      }
    });
  }

  private async receiveTask(): Promise<TaskDefinition | undefined> {
    const query: MongoFilter<TaskDefinition> = {
      _id: {$in: Array.from(this.taskWorkers.keys())},
      nextScheduledTime: {$lt: new Date()},
      $or: [
        {receivedTime: {$lt: new Date(Date.now() - this.processingTimeout)}},
        {receivedTime: {$exists: false}}
      ]
    };

    const update = {
      $set: {
        recievedTime: new Date()
      }
    };

    try {
      const collection = await this.getMongoCollection();
      const result = await collection.findOneAndUpdate(query, update, {returnDocument: 'after'});

      return result ?? undefined;
    } catch (error: any) {
      this.errorHandler(error);
      return undefined;
    }
  }

  private async updateTask(task: TaskDefinition, update: MongoUpdateFilter<TaskDefinition>) {
    const collection = await this.getMongoCollection();

    await collection.updateOne({_id: task._id}, update);

    if ((task.executionLog?.length ?? 0) >= 10) {
      await collection.updateOne({_id: task._id}, {$pop: {executionLog: -1}});
    }
  }


  private async insertTask(task: TaskDefinition) {
    const collection = await this.getMongoCollection();
    const result = await collection.insertOne(task);

    return result;
  }

  private async getMongoCollection(): Promise<MongoCollection<TaskDefinition>> {
    if (!this.databasePromise){
      throw new Error('No database configured');
    }

    const db = await this.databasePromise();
    return db.collection(this.collectionName);
  }

  private getFormattedTime(time: Date): string {
    return moment(time).tz(this.timezone).format('YYYY.MM.DD hh:mm:ss A');
  }

}



