import { useEffect } from 'react';

import { sdk } from '../../app/utils/client/lib/SDKClient';
import { getUserId } from '../lib/user';
import { Users } from '../stores';

const TIMEOUT = 15000;

type TypingUsersByRoom = Map<string, Map<string, NodeJS.Timeout>>;

const typingUsersEmitter = {
	listeners: new Set<() => void>(),
	emit() {
		this.listeners.forEach((listener) => listener());
	},
	subscribe(listener: () => void) {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	},
};

const performingUsers = new Map<string, Map<string, NodeJS.Timeout>>();
let globalTypingStop: (() => void) | null = null;
let isInitialized = false;

const handleTimeout = (rid: string, username: string) => {
	const roomTyping = performingUsers.get(rid);
	if (roomTyping) {
		roomTyping.delete(username);
		performingUsers.set(rid, roomTyping);
		typingUsersEmitter.emit();
	}
};

const shownName = function (user: { name?: string; username?: string } | null | undefined): string | undefined {
	if (!user) return;
	return user.name || user.username;
};

const setupGlobalTypingSubscription = () => {
	if (globalTypingStop) return;

	const { stop } = sdk.stream('user-typing-global', ['user-typing'], (data) => {
		console.log('[useGlobalTyping] received:', data);
		const { rid, username, typing } = data as { rid: string; username: string; typing: boolean };

		const uid = getUserId();
		const user = uid ? Users.state.get(uid) : undefined;
		if (username === shownName(user)) {
			return;
		}

		const roomTyping = performingUsers.get(rid) || new Map();

		if (typing) {
			roomTyping.set(
				username,
				setTimeout(() => {
					handleTimeout(rid, username);
				}, TIMEOUT),
			);
		} else {
			const timeout = roomTyping.get(username);
			if (timeout) {
				clearTimeout(timeout);
				roomTyping.delete(username);
			}
		}

		performingUsers.set(rid, roomTyping);
		typingUsersEmitter.emit();
	});

	globalTypingStop = stop;
	isInitialized = true;
};

export const getGlobalTypingUsers = (): TypingUsersByRoom => performingUsers;

export const subscribeToGlobalTyping = (callback: () => void): (() => void) => {
	return typingUsersEmitter.subscribe(callback);
};

export const useGlobalTyping = (): void => {
	useEffect(() => {
		if (!isInitialized) {
			setupGlobalTypingSubscription();
		}
	}, []);
};
