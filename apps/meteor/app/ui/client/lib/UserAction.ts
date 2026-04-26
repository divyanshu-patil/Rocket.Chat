import type { IExtras, IRoomActivity, IUser } from '@rocket.chat/core-typings';
import { Emitter } from '@rocket.chat/emitter';
import { debounce } from 'lodash';

import { settings } from '../../../../client/lib/settings';
import { getUser, getUserId } from '../../../../client/lib/user';
import { Users } from '../../../../client/stores';
import { sdk } from '../../../utils/client/lib/SDKClient';

const TIMEOUT = 15000;
const RENEW = TIMEOUT / 3;

const USER_ACTIVITY = 'user-activity';

export const USER_ACTIVITIES = {
	USER_RECORDING: 'user-recording',
	USER_TYPING: 'user-typing',
	USER_UPLOADING: 'user-uploading',
	USER_PLAYING: 'user-playing',
};

const activityTimeouts = new Map();
const activityRenews = new Map();
const continuingIntervals = new Map();
const roomActivities = new Map<string, Set<string>>();
const rooms = new Map<string, (username: string, activityType: string[], extras?: object | undefined) => void>();

const performingUsers = new Map<string, IRoomActivity>();
const performingUsersEmitter = new Emitter<{ changed: void }>();

// Global subscription to receive typing events from ALL rooms (even when not subscribed to room)
let globalTypingStop: (() => void) | null = null;

const setupGlobalTypingSubscription = () => {
	if (globalTypingStop) return;

	const { stop } = sdk.stream('user-typing-global', ['user-typing'], (data: { rid: string; username: string; typing: boolean }) => {
		console.log('[user-typing-global] received:', data);
		const { rid, username, typing } = data;

		// Skip if this is our own typing event
		const uid = getUserId();
		const user = uid ? Users.state.get(uid) : undefined;
		if (username === shownName(user)) {
			return;
		}

		// Update the performingUsers map for this room
		const roomActivities = { ...performingUsers.get(rid) };
		roomActivities[USER_ACTIVITIES.USER_TYPING] = roomActivities[USER_ACTIVITIES.USER_TYPING] || new Map();

		if (typing) {
			(roomActivities[USER_ACTIVITIES.USER_TYPING] as Map<string, NodeJS.Timeout>).set(
				username,
				setTimeout(() => {
					handleGlobalTypingTimeout(rid, username);
				}, TIMEOUT),
			);
		} else {
			const timeout = (roomActivities[USER_ACTIVITIES.USER_TYPING] as Map<string, NodeJS.Timeout>).get(username);
			if (timeout) {
				clearTimeout(timeout);
				(roomActivities[USER_ACTIVITIES.USER_TYPING] as Map<string, NodeJS.Timeout>).delete(username);
			}
		}

		performingUsers.set(rid, roomActivities);
		performingUsersEmitter.emit('changed');
	});

	globalTypingStop = stop;
};

const handleGlobalTypingTimeout = (rid: string, username: string) => {
	const roomActivities = { ...performingUsers.get(rid) };
	roomActivities[USER_ACTIVITIES.USER_TYPING] = roomActivities[USER_ACTIVITIES.USER_TYPING] || new Map();
	(roomActivities[USER_ACTIVITIES.USER_TYPING] as Map<string, NodeJS.Timeout>).delete(username);
	performingUsers.set(rid, roomActivities);
	performingUsersEmitter.emit('changed');
};

// Initialize global subscription on module load
setupGlobalTypingSubscription();

const shownName = function (user: IUser | null | undefined): string | undefined {
	if (!user) {
		return;
	}
	if (settings.peek('UI_Use_Real_Name')) {
		return user.name;
	}
	return user.username;
};

const emitActivities = debounce(async (rid: string, extras: IExtras): Promise<void> => {
	const activities = roomActivities.get(extras?.tmid || rid) || new Set();
	// Publish to notify-room stream (only subscribed users)
	sdk.publish('notify-room', [`${rid}/${USER_ACTIVITY}`, shownName(getUser()), [...activities], extras]);
	// Also publish to user-typing stream (all users in room, subscribed or not)
	const username = shownName(getUser());
	const isTyping = activities.has(USER_ACTIVITIES.USER_TYPING);
	console.log('[user-typing] publishing:', rid, { username, typing: isTyping });
	sdk.publish('user-typing', [`${rid}/user-typing`, { username, typing: isTyping }]);
}, 500);

function handleStreamAction(rid: string, username: string, activityTypes: string[], extras?: IExtras): void {
	rid = extras?.tmid || rid;
	const roomActivities = { ...performingUsers.get(rid) };

	for (const [, activity] of Object.entries(USER_ACTIVITIES)) {
		roomActivities[activity] = roomActivities[activity] || new Map();
		const users = roomActivities[activity];
		const timeout = users[username];

		if (timeout) {
			clearTimeout(timeout);
		}

		if (activityTypes.includes(activity)) {
			activityTypes.splice(activityTypes.indexOf(activity), 1);
			users[username] = setTimeout(() => handleStreamAction(rid, username, activityTypes, extras), TIMEOUT);
		} else {
			delete users[username];
		}
	}

	performingUsers.set(rid, roomActivities);
	performingUsersEmitter.emit('changed');
}
export const UserAction = new (class {
	addStream(rid: string): () => void {
		if (rooms.get(rid)) {
			throw new Error('UserAction - addStream should only be called once per room');
		}

		const handler = function (username: string, activityType: string[], extras?: object): void {
			const uid = getUserId();
			const user = uid ? Users.state.get(uid) : undefined;

			if (username === shownName(user)) {
				return;
			}
			handleStreamAction(rid, username, activityType, extras);
		};
		rooms.set(rid, handler);

		const { stop: stopActivity } = sdk.stream('notify-room', [`${rid}/${USER_ACTIVITY}`], handler);

		// Subscribe to user-typing stream (receives typing events from all users in room, subscribed or not)
		const { stop: stopTyping } = sdk.stream('user-typing-global', [`user-typing`], (data: { username: string; typing: boolean }) => {
			console.log('[user-typing] received:', rid, data);
			const activityType = data.typing ? [USER_ACTIVITIES.USER_TYPING] : [];
			handleStreamAction(rid, data.username, activityType);
		});

		return () => {
			if (!rooms.get(rid)) {
				return;
			}
			stopActivity();
			stopTyping();
			rooms.delete(rid);
		};
	}

	performContinuously(rid: string, activityType: string, extras: IExtras = {}): void {
		const trid = extras?.tmid || rid;
		const key = `${activityType}-${trid}`;

		if (continuingIntervals.get(key)) {
			return;
		}
		this.start(rid, activityType, extras);

		continuingIntervals.set(
			key,
			setInterval(() => {
				this.start(rid, activityType, extras);
			}, RENEW),
		);
	}

	start(rid: string, activityType: string, extras: IExtras = {}): void {
		const trid = extras?.tmid || rid;
		const key = `${activityType}-${trid}`;

		if (activityRenews.get(key)) {
			return;
		}

		activityRenews.set(
			key,
			setTimeout(() => {
				clearTimeout(activityRenews.get(key));
				activityRenews.delete(key);
			}, RENEW),
		);

		const activities = roomActivities.get(trid) || new Set();
		activities.add(activityType);
		roomActivities.set(trid, activities);

		void emitActivities(rid, extras);

		if (activityTimeouts.get(key)) {
			clearTimeout(activityTimeouts.get(key));
			activityTimeouts.delete(key);
		}

		activityTimeouts.set(
			key,
			setTimeout(() => this.stop(trid, activityType, extras), TIMEOUT),
		);
		activityTimeouts.get(key);
	}

	stop(rid: string, activityType: string, extras: IExtras): void {
		const trid = extras?.tmid || rid;
		const key = `${activityType}-${trid}`;

		if (activityTimeouts.get(key)) {
			clearTimeout(activityTimeouts.get(key));
			activityTimeouts.delete(key);
		}
		if (activityRenews.get(key)) {
			clearTimeout(activityRenews.get(key));
			activityRenews.delete(key);
		}
		if (continuingIntervals.get(key)) {
			clearInterval(continuingIntervals.get(key));
			continuingIntervals.delete(key);
		}

		const activities = roomActivities.get(trid) || new Set();
		activities.delete(activityType);
		roomActivities.set(trid, activities);
		void emitActivities(rid, extras);
	}

	get(roomId: string): IRoomActivity | undefined {
		return performingUsers.get(roomId);
	}

	subscribe(onChanged: () => void): () => void {
		return performingUsersEmitter.on('changed', onChanged);
	}
})();
