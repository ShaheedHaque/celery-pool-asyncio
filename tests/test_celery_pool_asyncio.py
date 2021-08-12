#!/usr/bin/env python
import asyncio
import pathlib

root = pathlib.Path()
brocker = root / 'brocker'
brocker.mkdir(exist_ok=True)

dir_messages = brocker / 'messages'
dir_messages.mkdir(exist_ok=True)

dir_out = brocker / 'out'
dir_out.mkdir(exist_ok=True)

dir_processed = brocker / 'processed'
dir_processed.mkdir(exist_ok=True)


async def tack_function(input_data):
    await asyncio.sleep(1)
    return input_data.upper()


def test_create_task():
    from celery.contrib.testing.app import TestApp
    app = TestApp(config={
        'broker_url': 'filesystem:// %s' % dir_messages,
        'broker_transport_options': {
            'data_folder_in': '%s' % dir_out,
            'data_folder_out': '%s' % dir_out,
            'data_folder_processed': '%s' % dir_processed,
        },
        'result_persistent': True,
        'worker_pool': 'celery_pool_asyncio:TaskPool',
    })
    wrapped = app.task(tack_function)
    app.register_task(wrapped)
    msg = 'hello, world!'
    loop = asyncio.get_event_loop()
    if False:
        #
        # This works.
        #
        task = wrapped(msg)
        reply = loop.run_until_complete(task)
    else:
        #
        # This times out.
        #
        task = wrapped.delay(msg)
        reply = loop.run_until_complete(task)
        reply = reply.get(timeout=10)
    loop.close()
    assert reply == msg.upper()
