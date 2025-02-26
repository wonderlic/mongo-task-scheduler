"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskScheduler = void 0;
const moment_timezone_1 = __importDefault(require("moment-timezone"));
const cron_parser_1 = __importDefault(require("cron-parser"));
class TaskScheduler {
    constructor() {
        this.taskWorkers = new Map();
        this.pollingIntervalId = undefined;
        // Options
        this.pollingInterval = 1000; // 1 second
        this.processingTimeout = 60 * 1000; // 1 minute
        this.timezone = moment_timezone_1.default.tz.guess();
        this.errorHandler = console.error;
        this.collectionName = '_scheduledTasks';
        this.poll = async () => {
            const task = await this.receiveTask();
            if (!task) {
                return;
            }
            try {
                const output = await this.processTask(task);
                return this.completeTask(task, output);
            }
            catch (error) {
                this.errorHandler(error);
                return this.releaseTask(task, error);
            }
        };
    }
    async scheduleTask(id, cronSchedule, taskWorker) {
        this.taskWorkers.set(id, taskWorker);
        this.startPolling();
        const task = await this.findTask(id);
        const interval = cron_parser_1.default.parseExpression(cronSchedule, { tz: this.timezone });
        const nextTime = interval.next().toDate();
        if (!task) {
            console.log(`next scheduled time for task '${id}' is ${this.getFormattedTime(nextTime)}`);
            await this.insertTask({
                _id: id,
                cronSchedule,
                nextScheduledTime: nextTime
            });
        }
        else {
            if (task.cronSchedule !== cronSchedule) {
                console.log(`next scheduled time for task '${task._id}' is ${this.getFormattedTime(nextTime)}`);
                await this.updateTask(task, {
                    $set: {
                        cronSchedule,
                        nextScheduledTime: nextTime
                    }
                });
            }
            else {
                console.log(`next scheduled time for task '${task._id}' is ${this.getFormattedTime(task.nextScheduledTime)}`);
            }
        }
    }
    stopPolling() {
        if (this.pollingIntervalId) {
            clearInterval(this.pollingIntervalId);
            this.pollingIntervalId = undefined;
        }
    }
    // Private Methods
    async findTask(id) {
        const collection = await this.getMongoCollection();
        const result = await collection.findOne({
            _id: id
        });
        return result ?? undefined;
    }
    startPolling() {
        if (!this.pollingIntervalId) {
            this.pollingIntervalId = setInterval(this.poll, this.pollingInterval);
        }
    }
    async processTask(task) {
        const taskWorker = this.taskWorkers.get(task._id);
        if (!taskWorker) {
            throw new Error(`No worker registered for scheduled task with id: ${task._id}`);
        }
        return taskWorker({
            signalProgress: () => {
                // !!!!!!!!!!! MUST DO !!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: Throw an error if already timed out.
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                return this.updateTask(task, {
                    $set: {
                        receivedTime: new Date()
                    }
                });
            }
        });
    }
    releaseTask(task, error) {
        const now = new Date();
        const interval = cron_parser_1.default.parseExpression(task.cronSchedule, {
            currentDate: (0, moment_timezone_1.default)().tz(this.timezone).add(1, 'seconds').toDate(),
            tz: this.timezone
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
    completeTask(task, output) {
        const now = new Date();
        const interval = cron_parser_1.default.parseExpression(task.cronSchedule, {
            currentDate: (0, moment_timezone_1.default)().tz(this.timezone).add(1, 'seconds').toDate(),
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
                    output: output ?? undefined,
                }
            }
        });
    }
    async receiveTask() {
        const query = {
            _id: { $in: Array.from(this.taskWorkers.keys()) },
            nextScheduledTime: { $lt: new Date() },
            $or: [
                { receivedTime: { $lt: new Date(Date.now() - this.processingTimeout) } },
                { receivedTime: { $exists: false } }
            ]
        };
        const update = {
            $set: {
                receivedTime: new Date()
            }
        };
        try {
            const collection = await this.getMongoCollection();
            const result = await collection.findOneAndUpdate(query, update, { returnDocument: 'after' });
            return result ?? undefined;
        }
        catch (error) {
            this.errorHandler(error);
            return undefined;
        }
    }
    async updateTask(task, update) {
        const collection = await this.getMongoCollection();
        await collection.updateOne({ _id: task._id }, update);
        if ((task.executionLog?.length ?? 0) >= 10) {
            await collection.updateOne({ _id: task._id }, { $pop: { executionLog: -1 } });
        }
    }
    async insertTask(task) {
        const collection = await this.getMongoCollection();
        const result = await collection.insertOne(task);
        return result;
    }
    async getMongoCollection() {
        if (!this.databasePromise) {
            throw new Error('No database configured');
        }
        const db = await this.databasePromise();
        return db.collection(this.collectionName);
    }
    getFormattedTime(time) {
        return (0, moment_timezone_1.default)(time).tz(this.timezone).format('YYYY.MM.DD hh:mm:ss A');
    }
}
exports.TaskScheduler = TaskScheduler;
