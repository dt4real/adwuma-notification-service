import http from 'http';
import 'express-async-errors';

import { winstonLogger } from '@dt4real/adwuma-common';
import { Logger } from 'winston';
import { config } from '@notifications/config';
import { Application } from 'express';
import { healthRoutes } from '@notifications/routes';
import { createConnection } from '@notifications/queues/connection';
import { Channel } from 'amqplib';
import { consumeAuthEmailMessages, consumeOrderEmailMessages } from '@notifications/queues/email.consumer';
import { checkConnection } from '@notifications/elasticsearch';


const log: Logger = winstonLogger(`${config.ELASTIC_SEARCH_URL}`, 'notificationServer', 'debug');

export function start(app: Application): void {
	startServer(app);
	app.use('', healthRoutes());
	startQueues();
	startElasticSearch();
}

async function startQueues(): Promise<void> {
	const emailChannel: Channel = await createConnection() as Channel;
	await consumeAuthEmailMessages(emailChannel);
	await consumeOrderEmailMessages(emailChannel);
}

function startElasticSearch(): void {
	checkConnection();
}

function startServer(app: Application): void {
	try {
		const httpServer: http.Server = new http.Server(app);
		log.info(`Worker with process id of ${process.pid} on notification server has started`);
		httpServer.listen(config.SERVER_PORT, () => {
			log.info(`Notification server running on port ${config.SERVER_PORT}`);
		});
	} catch (error) {
		log.log('error', 'NotificationService startServer() method:', error);
	}
}