import type { IRoom, IUser } from '@rocket.chat/core-typings';
import { NotificationQueue, Subscriptions } from '@rocket.chat/models';

import { callbacks } from './callbacks';
import { notifyOnSubscriptionChangedByRoomIdAndUserId } from '../../app/lib/server/lib/notifyListener';
import { Push } from '../../app/push/server';
import PushNotification from '../../app/push-notifications/server/lib/PushNotification';

export async function readMessages(room: IRoom, uid: IUser['_id'], readThreads: boolean): Promise<void> {
	await callbacks.run('beforeReadMessages', room._id, uid);

	const projection = { ls: 1, tunread: 1, alert: 1, ts: 1 };
	const sub = await Subscriptions.findOneByRoomIdAndUserId(room._id, uid, { projection });
	if (!sub) {
		throw new Error('error-invalid-subscription');
	}

	// do not mark room as read if there are still unread threads
	const alert = !!(sub.alert && !readThreads && sub.tunread && sub.tunread.length > 0);

	const setAsReadResponse = await Subscriptions.setAsReadByRoomIdAndUserId(room._id, uid, readThreads, alert);
	if (setAsReadResponse.modifiedCount) {
		void notifyOnSubscriptionChangedByRoomIdAndUserId(room._id, uid);
	}

	await NotificationQueue.clearQueueByUserId(uid);

	console.log('Creating read push with notId', PushNotification.getNotificationId(room._id));

	await Push.send({
		from: 'push',
		userId: uid,
		notId: PushNotification.getNotificationId(room._id),

		badge: 0,
		sound: '',
		priority: 5,

		title: '',
		text: '',

		payload: {
			host: Meteor.absoluteUrl(),
			notificationType: 'message-clear',
			rid: room._id,
		},
	});

	const lastSeen = sub.ls || sub.ts;
	callbacks.runAsync('afterReadMessages', room, { uid, lastSeen });
}
