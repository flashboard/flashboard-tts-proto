import asyncio
import io
import json
import re
from contextlib import AsyncExitStack, closing
from datetime import datetime
from threading import local
from typing import Callable, Dict, Iterable, List, Set, Tuple

import boto3
import paho.mqtt.client as mqtt  # type: ignore
import pygame
from asyncio_mqtt import Client
from botocore.exceptions import BotoCoreError, ClientError
from cachetools import LRUCache, cached
from loguru import logger
from paho.mqtt.client import topic_matches_sub

from config import settings


class ContextManager:
    def __init__(self) -> None:
        self._context: List = []

    @property
    def actions(self) -> Iterable:
        return self._context

    def add(self, action: "Action") -> None:
        self._context.append(action)

    def clear(self) -> None:
        self._context = []


class PollyWrapper:
    def __init__(self) -> None:
        self._polly = boto3.client("polly", **settings.aws)
        pygame.mixer.init()
        pygame.init()

    @cached(cache=LRUCache(maxsize=settings.cache_size))
    def _stream(self, text: str) -> bytes:
        try:
            response = self._polly.synthesize_speech(
                OutputFormat=settings.polly.output_format,
                Text=text,
                VoiceId=settings.polly.voice_id,
                Engine=settings.polly.engine,
            )
        except (BotoCoreError, ClientError) as e:
            raise Exception(f"Failed to get audio stream: {e}")

        if "AudioStream" in response:
            with closing(response["AudioStream"]):
                logger.debug("Streaming from Polly: '{}'", text)
                return response["AudioStream"].read()

    def play(self, text: str) -> None:
        logger.debug("Playing: '{}'", text)

        with io.BytesIO() as f:
            f.write(self._stream(text))
            f.seek(0)
            pygame.mixer.music.load(f)
            pygame.mixer.music.set_endevent(pygame.USEREVENT)
            pygame.event.set_allowed(pygame.USEREVENT)
            pygame.mixer.music.play()
            pygame.event.wait()


class Transport:
    def __init__(
        self,
        topic_subscribe: str = settings.topic,
    ) -> None:
        self._topic_filters: List[Tuple(str, Callable)] = []
        self._topic_subscribe: str = topic_subscribe

    def add_filter(self, topic: str, callback: Callable) -> None:
        self._topic_filters.append((topic, callback))

    async def _cancel_tasks(self, tasks):
        for task in tasks:
            if task.done():
                continue
            try:
                task.cancel()
                logger.debug("Task {} canceled", task)
                await task
            except asyncio.CancelledError as e:
                logger.warning("Error cancelling {}: {}", task, e)

    async def _handle_messages(self, messages: List[mqtt.MQTTMessage]) -> None:
        message: mqtt.MQTTMessage

        async for message in messages:
            for sub, callback in self._topic_filters:
                if topic_matches_sub(sub, message.topic):
                    await callback(message)

    async def run(self) -> None:
        logger.debug("Running Transport main loop")

        async with AsyncExitStack() as stack:
            self._tasks: Set = set()
            stack.push_async_callback(self._cancel_tasks, self._tasks)

            self._client: Client = Client(**settings.mqtt)
            await stack.enter_async_context(self._client)

            logger.info("Transport connected")

            messages = await stack.enter_async_context(
                self._client.unfiltered_messages()
            )
            task = asyncio.create_task(self._handle_messages(messages))
            self._tasks.add(task)

            await self._client.subscribe(self._topic_subscribe, qos=2)

            await asyncio.gather(*self._tasks)


class Action:
    take_context: bool = True
    take_polly: bool = False

    def __init__(self, ctx: ContextManager, topic: str) -> None:
        self._topic: str = topic
        self._ctx: ContextManager = ctx

        logger.info("{} registered on {}", self.__class__.__name__, topic)

    @property
    def topic(self):
        return self._topic

    async def __call__(self, message: mqtt.MQTTMessage) -> None:
        self._ctx.add(self)


class LogStatusAction(Action):
    async def __call__(self, message: mqtt.MQTTMessage) -> None:
        payload: str = message.payload.decode()
        topic: str = message.topic
        timestamp: int = message.timestamp

        logger.info("[{}] {}: {}", datetime.fromtimestamp(timestamp), topic, payload)

        await super().__call__(message)


class LogTagAction(Action):
    async def __call__(self, message: mqtt.MQTTMessage) -> None:
        data: Dict = json.loads(message.payload)
        mac: str = data.get("mac")
        tag: str = data.get("tag")

        if not tag:
            return

        logger.info("[{}] {}", mac, tag)

        await super().__call__(message)


class SpeakTagAction(Action):
    take_polly: bool = True

    def __init__(
        self,
        ctx: ContextManager,
        topic: str,
        polly: PollyWrapper,
        tag_id: str,
        text: str,
    ) -> None:
        super().__init__(ctx, topic)

        self._polly: PollyWrapper = polly
        self._tag_id: str = tag_id
        self._text: str = text
        self._dedup: Dict = {}

        logger.info(
            "{} registered on tag '{}' -> '{}'",
            self.__class__.__name__,
            self._tag_id,
            self._text,
        )

    @property
    def text(self):
        return self._text

    def _tag_matches(self, message: mqtt.MQTTMessage) -> bool:
        if message.dup:
            return False

        data: Dict = json.loads(message.payload)
        tag: str = data.get("tag")
        mac: str = data.get("mac")

        if not tag:
            return False

        if mac in self._dedup:
            return False

        self._dedup[mac] = tag

        return tag == self._tag_id

    async def __call__(self, message: mqtt.MQTTMessage) -> None:
        if not self._tag_matches(message):
            return

        self._polly.play(self._text)

        await super().__call__(message)


class SpeakSentenceAction(SpeakTagAction):
    def __init__(
        self,
        ctx: ContextManager,
        topic: str,
        polly: PollyWrapper,
        tag_id: str,
        text: str = " ",
        clear_context: bool = True,
    ) -> None:
        super().__init__(ctx, topic, polly, tag_id, text)

        self._clear_context: bool = clear_context

    async def __call__(self, message: mqtt.MQTTMessage) -> None:
        if not self._tag_matches(message):
            return

        text: str = ""

        text = self._text.join(
            [f"{a.text}" for a in self._ctx.actions if isinstance(a, SpeakTagAction)]
        )

        if len(text):
            self._polly.play(text)

        if self._clear_context:
            self._ctx.clear()


class Flashboard:
    def __init__(self, transport: Transport, actions: List[Action]) -> None:
        self._transport: Transport = transport
        self._actions: List[Action] = actions

        for action in self._actions:
            self._transport.add_filter(action.topic, action)

    async def run(self) -> None:
        await self._transport.run()


class ActionManager:
    def __init__(self, ctx: ContextManager, polly: PollyWrapper) -> None:
        self._ctx: ContextManager = ctx
        self._polly: PollyWrapper = polly

        self._actions: List = [
            LogTagAction,
            LogStatusAction,
            SpeakTagAction,
            SpeakSentenceAction,
        ]

    def load_from_settings(self) -> List[Action]:
        for action in settings.actions:
            for action_class in self._actions:
                if action_class.__name__ == action.action:
                    kw: Dict = dict(getattr(action, "args", {})).copy()

                    kw["topic"] = action.topic

                    if getattr(action_class, "take_context", False):
                        kw["ctx"] = self._ctx

                    if getattr(action_class, "take_polly", False):
                        kw["polly"] = self._polly

                    try:
                        yield action_class(**kw)
                    except Exception as e:
                        logger.warning(
                            "Could not instantiate {}: {}", action_class.__name__, e
                        )


async def main():
    pw: PollyWrapper = PollyWrapper()
    ctx: ContextManager = ContextManager()

    actions: List[Action] = ActionManager(ctx, pw).load_from_settings()

    tr: Transport = Transport()
    fb: Flashboard = Flashboard(tr, actions)
    reconnect_interval: int = 3

    while True:
        try:
            await fb.run()
        except Exception as e:
            logger.warning(
                'Error "{}". Reconnecting in {} seconds.', e, reconnect_interval
            )
        finally:
            await asyncio.sleep(reconnect_interval)


if __name__ == "__main__":
    asyncio.run(main())
