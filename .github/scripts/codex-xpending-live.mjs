import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import clientModule from '../../packages/client/dist/index.js';

const { createClient } = clientModule;

async function connect(RESP) {
  for (let attempt = 0; attempt < 30; attempt++) {
    const client = createClient({
      RESP,
      socket: { host: '127.0.0.1', port: 6399, reconnectStrategy: false, connectTimeout: 1000 }
    });
    client.on('error', () => {});
    try {
      await client.connect();
      return client;
    } catch (error) {
      if (client.isOpen) client.destroy();
      if (attempt === 29) throw error;
      await delay(500);
    }
  }
}

async function verify(RESP) {
  const client = await connect(RESP);
  const key = `codex:xpending:${process.pid}:resp${RESP}`;
  const group = 'group';
  try {
    const info = await client.info('server');
    console.log(`RESP ${RESP}; ${info.match(/^redis_version:.*$/m)?.[0]}`);
    await client.xGroupCreate(key, group, '0', { MKSTREAM: true });
    const emptyId = await client.xAdd(key, '*', { field: 'empty-consumer' });
    await client.xReadGroup(group, '', { key, id: '>' }, { COUNT: 1 });
    const otherId = await client.xAdd(key, '*', { field: 'other-consumer' });
    await client.xReadGroup(group, 'other', { key, id: '>' }, { COUNT: 1 });

    const cases = [
      ['omitted', undefined, [emptyId, otherId]],
      ['undefined', { consumer: undefined }, [emptyId, otherId]],
      ['empty string', { consumer: '' }, [emptyId]],
      ['empty buffer', { consumer: Buffer.alloc(0) }, [emptyId]],
      ['nonempty string', { consumer: 'other' }, [otherId]],
      ['nonempty buffer', { consumer: Buffer.from('other') }, [otherId]],
      ['empty string with IDLE 0', { IDLE: 0, consumer: '' }, [emptyId]],
      ['empty buffer with IDLE 0', { IDLE: 0, consumer: Buffer.alloc(0) }, [emptyId]],
      ['nonempty string with IDLE 0', { IDLE: 0, consumer: 'other' }, [otherId]],
      ['nonempty buffer with IDLE 0', { IDLE: 0, consumer: Buffer.from('other') }, [otherId]]
    ];
    for (const [name, options, ids] of cases) {
      const reply = options === undefined
        ? await client.xPendingRange(key, group, '-', '+', 10)
        : await client.xPendingRange(key, group, '-', '+', 10, options);
      assert.deepEqual(reply.map(item => item.id), ids, `RESP ${RESP}: ${name}`);
      assert.deepEqual(reply.map(item => item.consumer), ids.map(id => id === emptyId ? '' : 'other'));
      console.log(`PASS RESP ${RESP}: ${name}`);
    }
  } finally {
    if (client.isReady) await client.del(key);
    if (client.isOpen) client.destroy();
  }
}

(async () => {
  await verify(2);
  await verify(3);
  console.log('20 live XPENDING consumer scenarios passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
